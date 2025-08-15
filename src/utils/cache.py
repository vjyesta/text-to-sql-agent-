"""
Query caching utilities.
Implements an intelligent caching system for SQL queries and results.
"""

import json
import hashlib
import pickle
import os
import time
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timedelta
from collections import OrderedDict
import logging

logger = logging.getLogger(__name__)


class QueryCache:
    """
    Intelligent caching system for SQL queries and their results.
    Supports multiple caching strategies and automatic cleanup.
    """
    
    def __init__(self, 
                 cache_dir: str = 'cache',
                 max_size: int = 100,
                 ttl_seconds: int = 3600,
                 strategy: str = 'lru'):
        """
        Initialize the query cache.
        
        Args:
            cache_dir: Directory to store cache files
            max_size: Maximum number of cached queries
            ttl_seconds: Time-to-live for cache entries in seconds
            strategy: Caching strategy ('lru', 'lfu', 'ttl')
        """
        self.cache_dir = cache_dir
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self.strategy = strategy
        
        # Create cache directory if it doesn't exist
        os.makedirs(cache_dir, exist_ok=True)
        
        # In-memory cache storage
        self.memory_cache = OrderedDict()
        self.access_counts = {}  # For LFU strategy
        self.access_times = {}   # For TTL strategy
        
        # Cache statistics
        self.stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0,
            'total_queries': 0
        }
        
        # Load persistent cache metadata
        self.cache_metadata_file = os.path.join(cache_dir, 'cache_metadata.json')
        self.load_metadata()
        
        logger.info(f"QueryCache initialized with strategy: {strategy}")
    
    def get_cache_key(self, query: str, params: Optional[Dict] = None) -> str:
        """
        Generate a unique cache key for a query.
        
        Args:
            query: Natural language or SQL query
            params: Optional parameters
            
        Returns:
            Cache key string
        """
        # Normalize the query
        normalized_query = query.lower().strip()
        
        # Include parameters in the key
        if params:
            param_str = json.dumps(params, sort_keys=True)
            normalized_query += param_str
        
        # Generate hash
        return hashlib.sha256(normalized_query.encode()).hexdigest()
    
    def get(self, query: str, params: Optional[Dict] = None) -> Optional[Dict[str, Any]]:
        """
        Retrieve a cached result.
        
        Args:
            query: Query string
            params: Optional parameters
            
        Returns:
            Cached result or None if not found
        """
        self.stats['total_queries'] += 1
        cache_key = self.get_cache_key(query, params)
        
        # Check memory cache first
        if cache_key in self.memory_cache:
            # Check if expired
            if self._is_expired(cache_key):
                self._remove_from_cache(cache_key)
                self.stats['misses'] += 1
                return None
            
            # Update access information
            self._update_access(cache_key)
            
            self.stats['hits'] += 1
            result = self.memory_cache[cache_key]
            
            logger.debug(f"Cache hit for key: {cache_key[:8]}...")
            return result
        
        # Check persistent cache
        cached_result = self._load_from_disk(cache_key)
        if cached_result:
            # Load into memory cache
            self._add_to_memory_cache(cache_key, cached_result)
            self.stats['hits'] += 1
            logger.debug(f"Disk cache hit for key: {cache_key[:8]}...")
            return cached_result
        
        self.stats['misses'] += 1
        logger.debug(f"Cache miss for key: {cache_key[:8]}...")
        return None
    
    def set(self, 
            query: str, 
            result: Dict[str, Any],
            params: Optional[Dict] = None,
            ttl_override: Optional[int] = None) -> bool:
        """
        Store a query result in cache.
        
        Args:
            query: Query string
            result: Result to cache
            params: Optional parameters
            ttl_override: Override default TTL
            
        Returns:
            True if successfully cached
        """
        try:
            cache_key = self.get_cache_key(query, params)
            
            # Add metadata
            cache_entry = {
                'query': query,
                'result': result,
                'params': params,
                'timestamp': time.time(),
                'ttl': ttl_override or self.ttl_seconds
            }
            
            # Check cache size and evict if necessary
            if len(self.memory_cache) >= self.max_size:
                self._evict()
            
            # Add to memory cache
            self._add_to_memory_cache(cache_key, cache_entry)
            
            # Save to disk for persistence
            self._save_to_disk(cache_key, cache_entry)
            
            logger.debug(f"Cached result for key: {cache_key[:8]}...")
            return True
            
        except Exception as e:
            logger.error(f"Failed to cache result: {e}")
            return False
    
    def invalidate(self, pattern: Optional[str] = None):
        """
        Invalidate cache entries matching a pattern.
        
        Args:
            pattern: Optional pattern to match (invalidates all if None)
        """
        if pattern is None:
            # Clear all cache
            self.memory_cache.clear()
            self.access_counts.clear()
            self.access_times.clear()
            
            # Clear disk cache
            for file in os.listdir(self.cache_dir):
                if file.endswith('.cache'):
                    os.remove(os.path.join(self.cache_dir, file))
            
            logger.info("Cleared all cache entries")
        else:
            # Invalidate matching entries
            keys_to_remove = []
            for key, entry in self.memory_cache.items():
                if pattern.lower() in entry['query'].lower():
                    keys_to_remove.append(key)
            
            for key in keys_to_remove:
                self._remove_from_cache(key)
            
            logger.info(f"Invalidated {len(keys_to_remove)} cache entries matching '{pattern}'")
    
    def _add_to_memory_cache(self, key: str, entry: Dict[str, Any]):
        """
        Add an entry to memory cache.
        
        Args:
            key: Cache key
            entry: Cache entry
        """
        self.memory_cache[key] = entry
        
        if self.strategy == 'lru':
            # Move to end (most recently used)
            self.memory_cache.move_to_end(key)
        
        # Initialize access tracking
        self.access_counts[key] = 1
        self.access_times[key] = time.time()
    
    def _update_access(self, key: str):
        """
        Update access information for a cache entry.
        
        Args:
            key: Cache key
        """
        if self.strategy == 'lru':
            # Move to end (most recently used)
            self.memory_cache.move_to_end(key)
        elif self.strategy == 'lfu':
            # Increment access count
            self.access_counts[key] = self.access_counts.get(key, 0) + 1
        
        self.access_times[key] = time.time()
    
    def _evict(self):
        """
        Evict entries based on the caching strategy.
        """
        if not self.memory_cache:
            return
        
        if self.strategy == 'lru':
            # Remove least recently used (first item)
            key_to_evict = next(iter(self.memory_cache))
        elif self.strategy == 'lfu':
            # Remove least frequently used
            key_to_evict = min(self.access_counts, key=self.access_counts.get)
        else:  # ttl or default
            # Remove oldest entry
            key_to_evict = min(self.access_times, key=self.access_times.get)
        
        self._remove_from_cache(key_to_evict)
        self.stats['evictions'] += 1
        
        logger.debug(f"Evicted cache entry: {key_to_evict[:8]}...")
    
    def _remove_from_cache(self, key: str):
        """
        Remove an entry from all cache stores.
        
        Args:
            key: Cache key
        """
        # Remove from memory
        if key in self.memory_cache:
            del self.memory_cache[key]
        if key in self.access_counts:
            del self.access_counts[key]
        if key in self.access_times:
            del self.access_times[key]
        
        # Remove from disk
        cache_file = os.path.join(self.cache_dir, f"{key}.cache")
        if os.path.exists(cache_file):
            os.remove(cache_file)
    
    def _is_expired(self, key: str) -> bool:
        """
        Check if a cache entry has expired.
        
        Args:
            key: Cache key
            
        Returns:
            True if expired, False otherwise
        """
        if key not in self.memory_cache:
            return True
        
        entry = self.memory_cache[key]
        ttl = entry.get('ttl', self.ttl_seconds)
        timestamp = entry.get('timestamp', 0)
        
        return (time.time() - timestamp) > ttl
    
    def _save_to_disk(self, key: str, entry: Dict[str, Any]):
        """
        Save a cache entry to disk.
        
        Args:
            key: Cache key
            entry: Cache entry
        """
        try:
            cache_file = os.path.join(self.cache_dir, f"{key}.cache")
            with open(cache_file, 'wb') as f:
                pickle.dump(entry, f)
        except Exception as e:
            logger.error(f"Failed to save cache to disk: {e}")
    
    def _load_from_disk(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Load a cache entry from disk.
        
        Args:
            key: Cache key
            
        Returns:
            Cache entry or None if not found
        """
        try:
            cache_file = os.path.join(self.cache_dir, f"{key}.cache")
            if os.path.exists(cache_file):
                with open(cache_file, 'rb') as f:
                    entry = pickle.load(f)
                
                # Check if expired
                ttl = entry.get('ttl', self.ttl_seconds)
                timestamp = entry.get('timestamp', 0)
                
                if (time.time() - timestamp) > ttl:
                    os.remove(cache_file)
                    return None
                
                return entry
        except Exception as e:
            logger.error(f"Failed to load cache from disk: {e}")
        
        return None
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get cache statistics.
        
        Returns:
            Dictionary of cache statistics
        """
        hit_rate = 0
        if self.stats['total_queries'] > 0:
            hit_rate = (self.stats['hits'] / self.stats['total_queries']) * 100
        
        return {
            'total_queries': self.stats['total_queries'],
            'hits': self.stats['hits'],
            'misses': self.stats['misses'],
            'evictions': self.stats['evictions'],
            'hit_rate': f"{hit_rate:.1f}%",
            'cache_size': len(self.memory_cache),
            'max_size': self.max_size,
            'strategy': self.strategy
        }
    
    def save_metadata(self):
        """Save cache metadata to disk."""
        try:
            metadata = {
                'stats': self.stats,
                'strategy': self.strategy,
                'cache_keys': list(self.memory_cache.keys()),
                'timestamp': datetime.now().isoformat()
            }
            
            with open(self.cache_metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
                
        except Exception as e:
            logger.error(f"Failed to save cache metadata: {e}")
    
    def load_metadata(self):
        """Load cache metadata from disk."""
        try:
            if os.path.exists(self.cache_metadata_file):
                with open(self.cache_metadata_file, 'r') as f:
                    metadata = json.load(f)
                    self.stats = metadata.get('stats', self.stats)
                    logger.info(f"Loaded cache metadata: {self.stats}")
        except Exception as e:
            logger.error(f"Failed to load cache metadata: {e}")
    
    def cleanup_expired(self):
        """Remove all expired cache entries."""
        expired_keys = []
        
        for key in list(self.memory_cache.keys()):
            if self._is_expired(key):
                expired_keys.append(key)
        
        for key in expired_keys:
            self._remove_from_cache(key)
        
        # Clean up disk cache
        for file in os.listdir(self.cache_dir):
            if file.endswith('.cache'):
                key = file[:-6]  # Remove .cache extension
                if key not in self.memory_cache:
                    file_path = os.path.join(self.cache_dir, file)
                    try:
                        # Check if file is expired
                        with open(file_path, 'rb') as f:
                            entry = pickle.load(f)
                        
                        ttl = entry.get('ttl', self.ttl_seconds)
                        timestamp = entry.get('timestamp', 0)
                        
                        if (time.time() - timestamp) > ttl:
                            os.remove(file_path)
                            expired_keys.append(key)
                    except:
                        # Remove corrupted cache files
                        os.remove(file_path)
        
        if expired_keys:
            logger.info(f"Cleaned up {len(expired_keys)} expired cache entries")
        
        return len(expired_keys)


class ResultCache:
    """
    Specialized cache for query results with compression support.
    """
    
    def __init__(self, cache_dir: str = 'cache/results', compress: bool = True):
        """
        Initialize the result cache.
        
        Args:
            cache_dir: Directory for cache storage
            compress: Whether to compress cached results
        """
        self.cache_dir = cache_dir
        self.compress = compress
        
        os.makedirs(cache_dir, exist_ok=True)
        
        # Track cached results
        self.cache_index = self._load_index()
    
    def cache_result(self,
                    query: str,
                    sql: str,
                    columns: List[str],
                    data: List[Tuple],
                    metadata: Optional[Dict] = None) -> str:
        """
        Cache a query result with metadata.
        
        Args:
            query: Natural language query
            sql: Generated SQL query
            columns: Column names
            data: Result data
            metadata: Optional metadata
            
        Returns:
            Cache ID
        """
        import uuid
        import gzip
        
        cache_id = str(uuid.uuid4())
        timestamp = datetime.now()
        
        cache_entry = {
            'id': cache_id,
            'query': query,
            'sql': sql,
            'columns': columns,
            'data': data,
            'metadata': metadata or {},
            'timestamp': timestamp.isoformat(),
            'row_count': len(data),
            'column_count': len(columns)
        }
        
        # Save to disk
        cache_file = os.path.join(self.cache_dir, f"{cache_id}.json")
        
        try:
            if self.compress:
                # Compress the data
                json_data = json.dumps(cache_entry, default=str)
                with gzip.open(cache_file + '.gz', 'wt', encoding='utf-8') as f:
                    f.write(json_data)
            else:
                with open(cache_file, 'w') as f:
                    json.dump(cache_entry, f, default=str)
            
            # Update index
            self.cache_index[cache_id] = {
                'query': query,
                'timestamp': timestamp.isoformat(),
                'row_count': len(data),
                'file': cache_file + ('.gz' if self.compress else '')
            }
            
            self._save_index()
            
            logger.info(f"Cached result with ID: {cache_id}")
            return cache_id
            
        except Exception as e:
            logger.error(f"Failed to cache result: {e}")
            return ""
    
    def get_result(self, cache_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a cached result by ID.
        
        Args:
            cache_id: Cache ID
            
        Returns:
            Cached result or None
        """
        import gzip
        
        if cache_id not in self.cache_index:
            return None
        
        cache_file = self.cache_index[cache_id]['file']
        
        try:
            if cache_file.endswith('.gz'):
                with gzip.open(cache_file, 'rt', encoding='utf-8') as f:
                    return json.load(f)
            else:
                with open(cache_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load cached result: {e}")
            return None
    
    def search_cache(self, 
                    query_pattern: Optional[str] = None,
                    date_from: Optional[datetime] = None,
                    date_to: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Search cached results.
        
        Args:
            query_pattern: Pattern to match in queries
            date_from: Start date filter
            date_to: End date filter
            
        Returns:
            List of matching cache entries
        """
        results = []
        
        for cache_id, info in self.cache_index.items():
            # Filter by query pattern
            if query_pattern and query_pattern.lower() not in info['query'].lower():
                continue
            
            # Filter by date
            timestamp = datetime.fromisoformat(info['timestamp'])
            if date_from and timestamp < date_from:
                continue
            if date_to and timestamp > date_to:
                continue
            
            results.append({
                'id': cache_id,
                'query': info['query'],
                'timestamp': info['timestamp'],
                'row_count': info['row_count']
            })
        
        # Sort by timestamp (newest first)
        results.sort(key=lambda x: x['timestamp'], reverse=True)
        
        return results
    
    def _load_index(self) -> Dict[str, Dict]:
        """Load the cache index."""
        index_file = os.path.join(self.cache_dir, 'index.json')
        
        if os.path.exists(index_file):
            try:
                with open(index_file, 'r') as f:
                    return json.load(f)
            except:
                pass
        
        return {}
    
    def _save_index(self):
        """Save the cache index."""
        index_file = os.path.join(self.cache_dir, 'index.json')
        
        try:
            with open(index_file, 'w') as f:
                json.dump(self.cache_index, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save cache index: {e}")


class CacheManager:
    """
    Manages multiple cache instances and provides a unified interface.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the cache manager.
        
        Args:
            config: Cache configuration
        """
        config = config or {}
        
        # Initialize different cache types
        self.query_cache = QueryCache(
            cache_dir=config.get('query_cache_dir', 'cache/queries'),
            max_size=config.get('max_query_cache_size', 100),
            ttl_seconds=config.get('query_ttl', 3600),
            strategy=config.get('cache_strategy', 'lru')
        )
        
        self.result_cache = ResultCache(
            cache_dir=config.get('result_cache_dir', 'cache/results'),
            compress=config.get('compress_results', True)
        )
        
        # Schedule periodic cleanup
        self.last_cleanup = time.time()
        self.cleanup_interval = config.get('cleanup_interval', 3600)  # 1 hour
        
        logger.info("CacheManager initialized")
    
    def get_cached_sql(self, query: str, params: Optional[Dict] = None) -> Optional[str]:
        """
        Get cached SQL for a natural language query.
        
        Args:
            query: Natural language query
            params: Optional parameters
            
        Returns:
            Cached SQL or None
        """
        self._periodic_cleanup()
        
        cached = self.query_cache.get(query, params)
        if cached:
            return cached.get('result', {}).get('sql_query')
        return None
    
    def cache_sql(self, 
                 query: str, 
                 sql: str, 
                 params: Optional[Dict] = None,
                 ttl: Optional[int] = None):
        """
        Cache a SQL query.
        
        Args:
            query: Natural language query
            sql: Generated SQL
            params: Optional parameters
            ttl: Time-to-live override
        """
        result = {'sql_query': sql}
        self.query_cache.set(query, result, params, ttl)
    
    def cache_query_result(self,
                          query: str,
                          sql: str,
                          columns: List[str],
                          data: List[Tuple],
                          metadata: Optional[Dict] = None) -> str:
        """
        Cache a complete query result.
        
        Args:
            query: Natural language query
            sql: Generated SQL
            columns: Column names
            data: Result data
            metadata: Optional metadata
            
        Returns:
            Cache ID
        """
        return self.result_cache.cache_result(query, sql, columns, data, metadata)
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get combined cache statistics.
        
        Returns:
            Dictionary of statistics
        """
        return {
            'query_cache': self.query_cache.get_statistics(),
            'result_cache': {
                'cached_results': len(self.result_cache.cache_index),
                'cache_dir': self.result_cache.cache_dir
            }
        }
    
    def clear_all(self):
        """Clear all caches."""
        self.query_cache.invalidate()
        
        # Clear result cache
        import shutil
        if os.path.exists(self.result_cache.cache_dir):
            shutil.rmtree(self.result_cache.cache_dir)
            os.makedirs(self.result_cache.cache_dir)
        
        self.result_cache.cache_index = {}
        self.result_cache._save_index()
        
        logger.info("All caches cleared")
    
    def _periodic_cleanup(self):
        """Perform periodic cleanup of expired entries."""
        current_time = time.time()
        
        if current_time - self.last_cleanup > self.cleanup_interval:
            self.query_cache.cleanup_expired()
            self.last_cleanup = current_time


# Global cache manager instance
cache_manager = CacheManager()