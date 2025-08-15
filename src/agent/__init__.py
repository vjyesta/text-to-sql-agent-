"""
Text-to-SQL Agent module.
Provides core agent functionality with optimization and validation.
"""

from typing import Dict, Any, Optional, List
from .core import TextToSQLAgent
from .optimizer import QueryOptimizer
from .validator import QueryValidator

__all__ = [
    'TextToSQLAgent',
    'QueryOptimizer',
    'QueryValidator',
    'EnhancedTextToSQLAgent'
]

__version__ = '1.0.0'


class EnhancedTextToSQLAgent(TextToSQLAgent):
    """
    Enhanced Text-to-SQL Agent with optimization and validation.
    
    This class combines the core agent with query optimization
    and validation for production-ready deployments.
    """
    
    def __init__(self, 
                 api_key: str, 
                 db_path: str = 'data/ecommerce.db',
                 model: str = 'gpt-4',
                 enable_optimization: bool = True,
                 enable_validation: bool = True,
                 optimization_level: str = 'standard'):
        """
        Initialize the enhanced agent.
        
        Args:
            api_key: OpenAI API key
            db_path: Path to database
            model: OpenAI model to use
            enable_optimization: Whether to enable query optimization
            enable_validation: Whether to enable query validation
            optimization_level: Level of optimization ('minimal', 'standard', 'aggressive')
        """
        # Initialize base agent
        super().__init__(api_key, db_path, model)
        
        # Initialize logger
        import logging
        self.logger = logging.getLogger(__name__)
        
        # Initialize components
        self.enable_optimization = enable_optimization
        self.enable_validation = enable_validation
        
        if enable_optimization:
            self.optimizer = QueryOptimizer(
                db_type='sqlite',
                max_limit=1000 if optimization_level != 'minimal' else 10000
            )
        
        if enable_validation:
            self.validator = QueryValidator(db_connection=self.conn)
        
        self.optimization_level = optimization_level
        
        # Track enhanced statistics
        self.enhanced_stats = {
            'queries_optimized': 0,
            'queries_validated': 0,
            'optimization_improvements': 0,
            'validation_failures': 0
        }
        
        self.logger.info(f"Enhanced agent initialized with optimization={enable_optimization}, validation={enable_validation}")
    
    def generate_sql(self, natural_language_query: str) -> Dict[str, Any]:
        """
        Generate SQL with optimization and validation.
        
        Args:
            natural_language_query: Natural language query
            
        Returns:
            Result dictionary with SQL and metadata
        """
        # Get base SQL from parent class
        result = super().generate_sql(natural_language_query)
        
        if not result['success']:
            return result
        
        sql_query = result['sql_query']
        
        # Validate the query
        if self.enable_validation:
            validation_result = self.validator.validate(sql_query)
            
            if not validation_result['is_valid']:
                self.enhanced_stats['validation_failures'] += 1
                return {
                    'success': False,
                    'sql_query': sql_query,
                    'error': f"Validation failed: {validation_result['summary']}",
                    'validation_details': validation_result,
                    'natural_language': natural_language_query
                }
            
            self.enhanced_stats['queries_validated'] += 1
            result['validation'] = validation_result
        
        # Optimize the query
        if self.enable_optimization:
            # Prepare context for optimization
            context = self._prepare_optimization_context()
            
            optimization_result = self.optimizer.optimize(sql_query, context)
            
            if optimization_result['is_optimized']:
                self.enhanced_stats['queries_optimized'] += 1
                self.enhanced_stats['optimization_improvements'] += optimization_result['improvement_score']
                
                sql_query = optimization_result['optimized_query']
                result['sql_query'] = sql_query
                result['optimization'] = optimization_result
                
                self.logger.info(f"Query optimized with {len(optimization_result['optimizations_applied'])} improvements")
        
        return result
    
    def process_question(self, question: str) -> Dict[str, Any]:
        """
        Process question with enhanced features.
        
        Args:
            question: Natural language question
            
        Returns:
            Complete result with data and metadata
        """
        # Get base result
        result = super().process_question(question)
        
        # Add enhanced statistics
        if result['success']:
            result['enhanced_stats'] = self.get_enhanced_statistics()
        
        return result
    
    def _prepare_optimization_context(self) -> Dict[str, Any]:
        """
        Prepare context information for query optimization.
        
        Returns:
            Context dictionary
        """
        context = {
            'default_limit': 100,
            'indexes': {},
            'table_sizes': {}
        }
        
        try:
            cursor = self.conn.cursor()
            
            # Get index information
            cursor.execute("""
                SELECT name, tbl_name, sql 
                FROM sqlite_master 
                WHERE type='index'
            """)
            
            for idx_name, table_name, sql in cursor.fetchall():
                if sql:  # Some indexes don't have SQL (e.g., PRIMARY KEY)
                    # Extract indexed columns (simplified)
                    columns = []
                    if '(' in sql and ')' in sql:
                        cols_str = sql[sql.index('(')+1:sql.index(')')]
                        columns = [col.strip() for col in cols_str.split(',')]
                    
                    context['indexes'][idx_name] = columns
            
            # Get table sizes (row counts)
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table'
            """)
            
            for (table_name,) in cursor.fetchall():
                if not table_name.startswith('sqlite_'):
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    count = cursor.fetchone()[0]
                    context['table_sizes'][table_name] = count
            
            # Add schema information
            context['schema'] = {}
            for table_name in context['table_sizes'].keys():
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = [row[1] for row in cursor.fetchall()]
                context['schema'][table_name] = {'columns': columns}
            
        except Exception as e:
            self.logger.warning(f"Could not prepare full optimization context: {e}")
        
        return context
    
    def validate_query(self, sql_query: str) -> Dict[str, Any]:
        """
        Validate a SQL query.
        
        Args:
            sql_query: SQL query to validate
            
        Returns:
            Validation result
        """
        if not self.enable_validation:
            return {'is_valid': True, 'message': 'Validation disabled'}
        
        return self.validator.validate(sql_query)
    
    def optimize_query(self, sql_query: str) -> Dict[str, Any]:
        """
        Optimize a SQL query.
        
        Args:
            sql_query: SQL query to optimize
            
        Returns:
            Optimization result
        """
        if not self.enable_optimization:
            return {'optimized_query': sql_query, 'is_optimized': False, 'message': 'Optimization disabled'}
        
        context = self._prepare_optimization_context()
        return self.optimizer.optimize(sql_query, context)
    
    def get_enhanced_statistics(self) -> Dict[str, Any]:
        """
        Get enhanced agent statistics.
        
        Returns:
            Statistics dictionary
        """
        base_stats = self.get_statistics()
        
        # Add enhanced statistics
        base_stats.update({
            'queries_optimized': self.enhanced_stats['queries_optimized'],
            'queries_validated': self.enhanced_stats['queries_validated'],
            'validation_failures': self.enhanced_stats['validation_failures'],
            'average_optimization_improvement': (
                self.enhanced_stats['optimization_improvements'] / self.enhanced_stats['queries_optimized']
                if self.enhanced_stats['queries_optimized'] > 0 else 0
            )
        })
        
        # Add component statistics
        if self.enable_optimization:
            base_stats['optimizer_stats'] = self.optimizer.get_statistics()
        
        if self.enable_validation:
            base_stats['validator_stats'] = self.validator.get_statistics()
        
        return base_stats
    
    def reset_statistics(self):
        """Reset all statistics."""
        # Reset base statistics
        self.clear_cache()
        self.clear_history()
        
        # Reset enhanced statistics
        self.enhanced_stats = {
            'queries_optimized': 0,
            'queries_validated': 0,
            'optimization_improvements': 0,
            'validation_failures': 0
        }
        
        # Reset component statistics
        if self.enable_optimization:
            self.optimizer.reset_statistics()
        
        if self.enable_validation:
            self.validator.reset_statistics()
        
        self.logger.info("All statistics reset")


# Import logger setup
import logging

# Configure module logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)