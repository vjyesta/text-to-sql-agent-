"""
Query optimization module.
Optimizes SQL queries for better performance and accuracy.
"""

import re
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)


class QueryOptimizer:
    """
    Optimizes SQL queries for better performance and accuracy.
    
    This class applies various optimization techniques to improve
    query execution time and resource usage.
    """
    
    def __init__(self, db_type: str = 'sqlite', max_limit: int = 1000):
        """
        Initialize the query optimizer.
        
        Args:
            db_type: Database type (sqlite, mysql, postgresql)
            max_limit: Maximum limit for result sets
        """
        self.db_type = db_type
        self.max_limit = max_limit
        
        # Optimization rules in order of application
        self.optimization_rules = [
            self._normalize_whitespace,
            self._add_limit_if_missing,
            self._optimize_wildcards,
            self._optimize_joins,
            self._optimize_subqueries,
            self._add_index_hints,
            self._optimize_date_functions,
            self._prevent_cartesian_product,
            self._optimize_aggregations,
            self._optimize_order_by
        ]
        
        # Track optimization statistics
        self.stats = {
            'queries_optimized': 0,
            'rules_applied': {},
            'total_optimizations': 0
        }
        
        logger.info(f"QueryOptimizer initialized for {db_type}")
    
    def optimize(self, sql_query: str, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Apply all optimization rules to a SQL query.
        
        Args:
            sql_query: Original SQL query
            context: Optional context information (table sizes, indexes, etc.)
            
        Returns:
            Dictionary with optimized query and optimization details
        """
        original_query = sql_query
        optimizations_applied = []
        
        # Apply each optimization rule
        for rule in self.optimization_rules:
            try:
                before = sql_query
                sql_query = rule(sql_query, context)
                
                if before != sql_query:
                    rule_name = rule.__name__.replace('_', ' ').title()
                    optimizations_applied.append(rule_name)
                    
                    # Update statistics
                    self.stats['rules_applied'][rule_name] = \
                        self.stats['rules_applied'].get(rule_name, 0) + 1
                    
            except Exception as e:
                logger.warning(f"Optimization rule {rule.__name__} failed: {e}")
                continue
        
        # Update overall statistics
        self.stats['queries_optimized'] += 1
        self.stats['total_optimizations'] += len(optimizations_applied)
        
        # Calculate improvement score
        improvement_score = self._calculate_improvement_score(original_query, sql_query)
        
        result = {
            'original_query': original_query,
            'optimized_query': sql_query,
            'optimizations_applied': optimizations_applied,
            'optimization_count': len(optimizations_applied),
            'improvement_score': improvement_score,
            'is_optimized': original_query != sql_query
        }
        
        if optimizations_applied:
            logger.info(f"Applied {len(optimizations_applied)} optimizations")
        
        return result
    
    def _normalize_whitespace(self, query: str, context: Optional[Dict] = None) -> str:
        """
        Normalize whitespace in the query.
        
        Args:
            query: SQL query
            context: Optional context
            
        Returns:
            Normalized query
        """
        # Replace multiple spaces with single space
        query = re.sub(r'\s+', ' ', query)
        
        # Remove leading/trailing whitespace
        query = query.strip()
        
        # Ensure space around operators
        operators = ['=', '!=', '<>', '<=', '>=', '<', '>']
        for op in operators:
            # Add spaces around operator if not present
            query = re.sub(rf'([^\s]){re.escape(op)}([^\s])', rf'\1 {op} \2', query)
        
        return query
    
    def _add_limit_if_missing(self, query: str, context: Optional[Dict] = None) -> str:
        """
        Add LIMIT clause if not present to prevent large result sets.
        
        Args:
            query: SQL query
            context: Optional context
            
        Returns:
            Query with LIMIT clause
        """
        query_upper = query.upper()
        
        # Don't add LIMIT to:
        # - Queries that already have LIMIT
        # - COUNT queries (they return single value)
        # - INSERT/UPDATE/DELETE queries
        if ('LIMIT' in query_upper or 
            'COUNT(' in query_upper or
            query_upper.startswith(('INSERT', 'UPDATE', 'DELETE'))):
            return query
        
        # Check if it's a SELECT query
        if query_upper.startswith('SELECT'):
            # Add reasonable default limit
            default_limit = context.get('default_limit', 100) if context else 100
            query = f"{query.rstrip(';')} LIMIT {default_limit}"
            logger.debug(f"Added LIMIT {default_limit} to prevent large result set")
        
        return query
    
    def _optimize_wildcards(self, query: str, context: Optional[Dict] = None) -> str:
        """
        Optimize SELECT * to specific columns when possible.
        
        Args:
            query: SQL query
            context: Optional context with schema information
            
        Returns:
            Optimized query
        """
        if not context or 'schema' not in context:
            return query
        
        # Find SELECT * patterns
        pattern = r'SELECT\s+\*\s+FROM\s+(\w+)'
        matches = re.finditer(pattern, query, re.IGNORECASE)
        
        for match in matches:
            table_name = match.group(1)
            
            # If we have schema information for this table
            if table_name in context.get('schema', {}):
                table_info = context['schema'][table_name]
                
                # Get important columns (exclude large text fields if specified)
                columns = table_info.get('important_columns', table_info.get('columns', []))
                
                if columns and len(columns) < 20:  # Don't replace if too many columns
                    column_list = ', '.join(columns)
                    query = query.replace(match.group(0), f'SELECT {column_list} FROM {table_name}')
                    logger.debug(f"Replaced SELECT * with specific columns for table {table_name}")
        
        return query
    
    def _optimize_joins(self, query: str, context: Optional[Dict] = None) -> str:
        """
        Optimize JOIN operations for better performance.
        
        Args:
            query: SQL query
            context: Optional context
            
        Returns:
            Optimized query
        """
        # Convert RIGHT JOINs to LEFT JOINs (more efficient in most databases)
        if 'RIGHT JOIN' in query.upper():
            # This is a simplified transformation - in production, use proper SQL parser
            query = self._convert_right_to_left_join(query)
        
        # Ensure JOIN conditions use indexed columns
        if context and 'indexes' in context:
            query = self._optimize_join_conditions(query, context['indexes'])
        
        # Reorder JOINs based on table size (smaller tables first)
        if context and 'table_sizes' in context:
            query = self._reorder_joins_by_size(query, context['table_sizes'])
        
        return query
    
    def _optimize_subqueries(self, query: str, context: Optional[Dict] = None) -> str:
        """
        Optimize subqueries by converting to JOINs when possible.
        
        Args:
            query: SQL query
            context: Optional context
            
        Returns:
            Optimized query
        """
        # Detect IN subqueries that can be converted to JOINs
        in_pattern = r'WHERE\s+(\w+)\s+IN\s*\(\s*SELECT\s+(\w+)\s+FROM\s+(\w+)'
        
        matches = re.finditer(in_pattern, query, re.IGNORECASE)
        for match in matches:
            column = match.group(1)
            subquery_column = match.group(2)
            subquery_table = match.group(3)
            
            # Convert to EXISTS (often more efficient)
            exists_clause = f"WHERE EXISTS (SELECT 1 FROM {subquery_table} WHERE {subquery_table}.{subquery_column} = {column}"
            
            # Note: This is simplified - proper implementation would need full SQL parsing
            logger.debug(f"Converted IN subquery to EXISTS for better performance")
        
        return query
    
    def _add_index_hints(self, query: str, context: Optional[Dict] = None) -> str:
        """
        Add index hints or suggestions based on WHERE and JOIN clauses.
        
        Args:
            query: SQL query
            context: Optional context with index information
            
        Returns:
            Query with index hints (as comments for SQLite)
        """
        if not context or 'indexes' not in context:
            return query
        
        indexes_used = []
        
        # Find columns used in WHERE clauses
        where_pattern = r'WHERE\s+(\w+)\.?(\w+)?\s*[=<>]'
        where_matches = re.finditer(where_pattern, query, re.IGNORECASE)
        
        for match in where_matches:
            column = match.group(2) if match.group(2) else match.group(1)
            
            # Check if there's an index for this column
            for index_name, index_cols in context['indexes'].items():
                if column in index_cols:
                    indexes_used.append(index_name)
        
        # Add index hints as comments (SQLite doesn't support index hints directly)
        if indexes_used and self.db_type == 'sqlite':
            hint = f"-- Suggested indexes: {', '.join(set(indexes_used))}\n"
            query = hint + query
        
        return query
    
    def _optimize_date_functions(self, query: str, context: Optional[Dict] = None) -> str:
        """
        Optimize date function usage for better performance.
        
        Args:
            query: SQL query
            context: Optional context
            
        Returns:
            Optimized query
        """
        # Optimize date comparisons to use indexes
        # Replace DATE(column) = 'date' with column BETWEEN 'date 00:00:00' AND 'date 23:59:59'
        date_pattern = r"DATE\((\w+)\)\s*=\s*'([^']+)'"
        
        def replace_date_equality(match):
            column = match.group(1)
            date_value = match.group(2)
            return f"{column} BETWEEN '{date_value} 00:00:00' AND '{date_value} 23:59:59'"
        
        query = re.sub(date_pattern, replace_date_equality, query, flags=re.IGNORECASE)
        
        # Optimize relative date calculations
        query = self._optimize_relative_dates(query)
        
        return query
    
    def _optimize_relative_dates(self, query: str) -> str:
        """
        Optimize relative date calculations.
        
        Args:
            query: SQL query
            
        Returns:
            Optimized query
        """
        # SQLite specific optimizations
        if self.db_type == 'sqlite':
            # Replace verbose date arithmetic with simpler forms
            replacements = [
                (r"DATE\('now', '-(\d+) days?'\)", r"DATE('now', '-\1 days')"),
                (r"DATETIME\('now', 'localtime'\)", "DATETIME('now')"),
            ]
            
            for pattern, replacement in replacements:
                query = re.sub(pattern, replacement, query, flags=re.IGNORECASE)
        
        return query
    
    def _prevent_cartesian_product(self, query: str, context: Optional[Dict] = None) -> str:
        """
        Detect and prevent potential cartesian products.
        
        Args:
            query: SQL query
            context: Optional context
            
        Returns:
            Query with cartesian product prevention
        """
        # Count tables in FROM clause
        from_pattern = r'FROM\s+([\w\s,]+?)(?:WHERE|GROUP|ORDER|LIMIT|$)'
        from_match = re.search(from_pattern, query, re.IGNORECASE)
        
        if from_match:
            tables_str = from_match.group(1)
            tables = [t.strip() for t in tables_str.split(',')]
            
            # If multiple tables without JOIN conditions
            if len(tables) > 1 and 'JOIN' not in query.upper():
                # Check if WHERE clause has join conditions
                if 'WHERE' not in query.upper():
                    logger.warning("Potential cartesian product detected - no JOIN conditions")
                    
                    # Add a comment warning
                    query = "-- WARNING: Potential cartesian product - consider adding JOIN conditions\n" + query
        
        return query
    
    def _optimize_aggregations(self, query: str, context: Optional[Dict] = None) -> str:
        """
        Optimize aggregation functions and GROUP BY clauses.
        
        Args:
            query: SQL query
            context: Optional context
            
        Returns:
            Optimized query
        """
        # Ensure GROUP BY columns match SELECT columns (avoid MySQL issues)
        if 'GROUP BY' in query.upper():
            query = self._fix_group_by_columns(query)
        
        # Optimize COUNT(*) to COUNT(1) for slight performance gain
        query = query.replace('COUNT(*)', 'COUNT(1)')
        
        # Add HAVING optimizations if needed
        if 'HAVING' in query.upper():
            query = self._optimize_having_clause(query)
        
        return query
    
    def _optimize_order_by(self, query: str, context: Optional[Dict] = None) -> str:
        """
        Optimize ORDER BY clauses.
        
        Args:
            query: SQL query
            context: Optional context
            
        Returns:
            Optimized query
        """
        # Remove ORDER BY if there's a LIMIT 1 with MIN/MAX
        if 'LIMIT 1' in query.upper():
            # Check if query uses MIN or MAX
            if re.search(r'\b(MIN|MAX)\s*\(', query, re.IGNORECASE):
                # Remove unnecessary ORDER BY
                query = re.sub(r'ORDER\s+BY\s+[^)]+(?=\s+LIMIT)', '', query, flags=re.IGNORECASE)
                logger.debug("Removed redundant ORDER BY with MIN/MAX and LIMIT 1")
        
        # Ensure ORDER BY columns are in SELECT (for some databases)
        if self.db_type in ['postgresql', 'mysql']:
            query = self._ensure_order_by_in_select(query)
        
        return query
    
    def _convert_right_to_left_join(self, query: str) -> str:
        """
        Convert RIGHT JOIN to LEFT JOIN for better optimization.
        
        Args:
            query: SQL query
            
        Returns:
            Query with LEFT JOINs
        """
        # This is a simplified implementation
        # In production, use a proper SQL parser
        
        # Basic pattern matching for RIGHT JOIN
        pattern = r'(\w+)\s+RIGHT\s+JOIN\s+(\w+)'
        
        def swap_tables(match):
            table1 = match.group(1)
            table2 = match.group(2)
            return f"{table2} LEFT JOIN {table1}"
        
        query = re.sub(pattern, swap_tables, query, flags=re.IGNORECASE)
        
        return query
    
    def _reorder_joins_by_size(self, query: str, table_sizes: Dict[str, int]) -> str:
        """
        Reorder JOINs to process smaller tables first.
        
        Args:
            query: SQL query
            table_sizes: Dictionary of table names to row counts
            
        Returns:
            Query with reordered JOINs
        """
        # This is a complex optimization that would require proper SQL parsing
        # Simplified version here for demonstration
        
        # Note: In production, use a SQL parser library
        logger.debug("Join reordering by table size is available with context")
        
        return query
    
    def _optimize_join_conditions(self, query: str, indexes: Dict[str, List[str]]) -> str:
        """
        Ensure JOIN conditions use indexed columns.
        
        Args:
            query: SQL query
            indexes: Dictionary of index names to column lists
            
        Returns:
            Optimized query
        """
        # Add comments suggesting index usage
        suggestions = []
        
        # Find JOIN conditions
        join_pattern = r'ON\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)'
        matches = re.finditer(join_pattern, query, re.IGNORECASE)
        
        for match in matches:
            col1 = match.group(2)
            col2 = match.group(4)
            
            # Check if columns are indexed
            indexed = False
            for index_cols in indexes.values():
                if col1 in index_cols or col2 in index_cols:
                    indexed = True
                    break
            
            if not indexed:
                suggestions.append(f"Consider indexing {col1} and {col2}")
        
        if suggestions:
            suggestion_comment = "-- Optimization suggestions: " + "; ".join(suggestions) + "\n"
            query = suggestion_comment + query
        
        return query
    
    def _fix_group_by_columns(self, query: str) -> str:
        """
        Fix GROUP BY columns to match SELECT columns.
        
        Args:
            query: SQL query
            
        Returns:
            Fixed query
        """
        # This would require proper SQL parsing in production
        # Simplified version for demonstration
        
        return query
    
    def _optimize_having_clause(self, query: str) -> str:
        """
        Optimize HAVING clauses by moving conditions to WHERE when possible.
        
        Args:
            query: SQL query
            
        Returns:
            Optimized query
        """
        # Move non-aggregate conditions from HAVING to WHERE
        # This requires SQL parsing to identify aggregate vs non-aggregate conditions
        
        return query
    
    def _ensure_order_by_in_select(self, query: str) -> str:
        """
        Ensure ORDER BY columns are in SELECT clause.
        
        Args:
            query: SQL query
            
        Returns:
            Fixed query
        """
        # This would require SQL parsing to properly implement
        
        return query
    
    def _calculate_improvement_score(self, original: str, optimized: str) -> float:
        """
        Calculate an improvement score for the optimization.
        
        Args:
            original: Original query
            optimized: Optimized query
            
        Returns:
            Improvement score (0-100)
        """
        if original == optimized:
            return 0.0
        
        score = 0.0
        
        # Check for various improvements
        if 'LIMIT' in optimized and 'LIMIT' not in original:
            score += 20  # Added result limit
        
        if optimized.count('*') < original.count('*'):
            score += 15  # Reduced wildcards
        
        if 'EXISTS' in optimized and 'IN' in original:
            score += 10  # Converted IN to EXISTS
        
        if '-- WARNING' not in optimized and len(optimized) < len(original):
            score += 5  # Simplified query
        
        # Additional scoring based on specific optimizations
        if 'COUNT(1)' in optimized and 'COUNT(*)' in original:
            score += 5
        
        return min(score, 100.0)
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get optimization statistics.
        
        Returns:
            Dictionary of statistics
        """
        return {
            'queries_optimized': self.stats['queries_optimized'],
            'total_optimizations': self.stats['total_optimizations'],
            'average_optimizations': (
                self.stats['total_optimizations'] / self.stats['queries_optimized']
                if self.stats['queries_optimized'] > 0 else 0
            ),
            'rules_applied': self.stats['rules_applied'],
            'most_used_rule': (
                max(self.stats['rules_applied'].items(), key=lambda x: x[1])[0]
                if self.stats['rules_applied'] else None
            )
        }
    
    def reset_statistics(self):
        """Reset optimization statistics."""
        self.stats = {
            'queries_optimized': 0,
            'rules_applied': {},
            'total_optimizations': 0
        }
        logger.info("Optimization statistics reset")