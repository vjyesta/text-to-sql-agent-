"""
Core Text-to-SQL Agent implementation.
This module contains the main agent class that handles natural language to SQL conversion.
"""

import os
import sqlite3
import json
import re
import logging
import time
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from openai import OpenAI
from tabulate import tabulate

# Set up logging for better debugging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TextToSQLAgent:
    """
    An intelligent agent that converts natural language to SQL queries.
    
    This class acts as the brain of our application, taking human-readable
    questions and converting them into database queries.
    """
    
    def __init__(self, api_key: str, db_path: str = 'data/ecommerce.db', model: str = 'gpt-4'):
        """
        Initialize the Text-to-SQL agent.
        
        Args:
            api_key: Your OpenAI API key for accessing GPT models
            db_path: Path to the SQLite database file
            model: OpenAI model to use (gpt-4, gpt-3.5-turbo, etc.)
        """
        # Initialize OpenAI client with your API key
        self.client = OpenAI(api_key=api_key)
        self.model = model
        
        # Store database path and create connection
        self.db_path = db_path
        self.conn = self._create_connection()
        
        # Load database schema for context
        self.schema = self._load_schema()
        
        # Keep track of conversation for context
        self.conversation_history = []
        
        # Cache for frequently used queries
        self.query_cache = {}
        
        logger.info(f"Agent initialized with database at {db_path}")
    
    def _create_connection(self) -> sqlite3.Connection:
        """
        Create a database connection with safety settings.
        
        Returns:
            SQLite connection object
        
        Raises:
            Exception: If database cannot be connected
        """
        try:
            # Create data directory if it doesn't exist
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            
            # Connect to database
            conn = sqlite3.connect(self.db_path)
            
            # Enable foreign keys for referential integrity
            conn.execute("PRAGMA foreign_keys = ON")
            
            # Set timeout to avoid lock issues
            conn.execute("PRAGMA busy_timeout = 5000")
            
            logger.info("Database connection established successfully")
            return conn
            
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def _load_schema(self) -> str:
        """
        Load the database schema information.
        
        This method tries to load schema from a saved file first,
        then falls back to extracting it directly from the database.
        """
        schema_file = 'data/schema_description.txt'
        
        if os.path.exists(schema_file):
            logger.info(f"Loading schema from {schema_file}")
            with open(schema_file, 'r') as f:
                return f.read()
        else:
            logger.info("Schema file not found, extracting from database")
            return self._extract_schema_from_db()
    
    def _extract_schema_from_db(self) -> str:
        """
        Extract schema directly from the database.
        
        This reads the actual table structures from SQLite's metadata.
        """
        cursor = self.conn.cursor()
        
        # Query SQLite's master table for all table definitions
        cursor.execute("""
            SELECT name, sql FROM sqlite_master 
            WHERE type='table' 
            ORDER BY name
        """)
        
        tables = cursor.fetchall()
        
        schema = "DATABASE SCHEMA:\n" + "="*50 + "\n\n"
        
        for table_name, table_sql in tables:
            if table_name.startswith('sqlite_'):
                continue  # Skip SQLite internal tables
                
            schema += f"TABLE: {table_name}\n"
            schema += f"{table_sql}\n"
            
            # Get column information with more detail
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            schema += "\nColumns:\n"
            for col in columns:
                col_id, name, dtype, notnull, default, pk = col
                nullable = "NOT NULL" if notnull else "NULL"
                primary = " (PRIMARY KEY)" if pk else ""
                default_val = f" DEFAULT {default}" if default else ""
                schema += f"  - {name}: {dtype} {nullable}{primary}{default_val}\n"
            
            # Get foreign key information
            cursor.execute(f"PRAGMA foreign_key_list({table_name})")
            foreign_keys = cursor.fetchall()
            
            if foreign_keys:
                schema += "\nForeign Keys:\n"
                for fk in foreign_keys:
                    schema += f"  - {fk[3]} -> {fk[2]}.{fk[4]}\n"
            
            # Get sample data for context
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 2")
            sample_data = cursor.fetchall()
            
            if sample_data:
                schema += "\nSample Data:\n"
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
                column_names = [description[0] for description in cursor.description]
                
                for row in sample_data:
                    row_dict = dict(zip(column_names, row))
                    schema += f"  {row_dict}\n"
            
            schema += "\n" + "-"*30 + "\n\n"
        
        return schema
    
    def generate_sql(self, natural_language_query: str) -> Dict[str, Any]:
        """
        Convert natural language query to SQL.
        
        Args:
            natural_language_query: The user's question in natural language
            
        Returns:
            Dictionary containing SQL query and metadata
        """
        start_time = time.time()
        
        # Check cache first
        cache_key = self._get_cache_key(natural_language_query)
        if cache_key in self.query_cache:
            logger.info("Using cached SQL query")
            cached_result = self.query_cache[cache_key].copy()
            cached_result['from_cache'] = True
            return cached_result
        
        # Build the prompt for the LLM
        system_prompt = self._build_system_prompt()
        user_prompt = self._build_user_prompt(natural_language_query)
        
        try:
            logger.info(f"Generating SQL for: {natural_language_query}")
            
            # Call OpenAI API
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.1,  # Low temperature for more deterministic output
                max_tokens=5000
            )
            
            sql_query = response.choices[0].message.content.strip()
            
            # Clean up the SQL query (remove markdown if present)
            sql_query = self._clean_sql_query(sql_query)
            
            # Validate the SQL query
            validation_result = self._validate_sql(sql_query)
            
            generation_time = time.time() - start_time
            
            result = {
                'success': validation_result['is_valid'],
                'sql_query': sql_query,
                'error': validation_result.get('error'),
                'natural_language': natural_language_query,
                'generation_time': generation_time,
                'from_cache': False
            }
            
            # Cache successful queries
            if result['success']:
                self.query_cache[cache_key] = result
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to generate SQL: {e}")
            return {
                'success': False,
                'sql_query': None,
                'error': str(e),
                'natural_language': natural_language_query,
                'generation_time': time.time() - start_time,
                'from_cache': False
            }
    
    def _build_system_prompt(self) -> str:
        """
        Build the system prompt for the LLM.
        
        This prompt teaches the model how to be an expert SQL generator.
        """
        return """You are an expert SQL query generator for an e-commerce database.
        Your task is to convert natural language questions into valid SQLite queries.
        
        Important rules:
        1. Generate ONLY valid SQLite syntax
        2. Use proper JOIN clauses when querying multiple tables
        3. Include appropriate WHERE, GROUP BY, ORDER BY clauses as needed
        4. For aggregations, use COUNT, SUM, AVG, MIN, MAX appropriately
        5. Always limit results to reasonable numbers (default to 10 unless specified)
        6. Use aliases for better readability
        7. Handle NULL values appropriately with COALESCE or IS NULL/IS NOT NULL
        8. Use DATE() and DATETIME() functions for date operations
        9. Return ONLY the SQL query without any explanation or markdown
        
        Common patterns:
        - Revenue calculations: SUM(order_items.subtotal) or SUM(orders.total_amount)
        - Best sellers: GROUP BY product, ORDER BY COUNT or SUM(quantity)
        - Date filters: Use DATE() function and BETWEEN for date ranges
        - Customer analysis: JOIN customers with orders and order_items
        - Inventory checks: products.stock_quantity for current stock
        - Status filters: Use orders.status for order states (pending, completed, shipped)
        
        SQLite specific functions to remember:
        - DATE('now') for current date
        - DATE('now', '-30 days') for date arithmetic
        - CAST(x AS type) for type conversion
        - ROUND(number, decimals) for rounding
        - IFNULL(value, default) for NULL handling"""
    
    def _build_user_prompt(self, query: str) -> str:
        """
        Build the user prompt with context.
        
        Args:
            query: The natural language query
            
        Returns:
            Formatted prompt for the LLM
        """
        # Include recent conversation context if available
        context = ""
        if self.conversation_history:
            recent = self.conversation_history[-3:]  # Last 3 queries for context
            context = "Recent queries for context:\n"
            for item in recent:
                context += f"- Question: {item['question']}\n"
                context += f"  SQL: {item['sql']}\n"
            context += "\n"
        
        return f"""Database Schema:
{self.schema}

{context}User Question: {query}

Generate a SQL query to answer this question. Return only the SQL query."""
    
    def _clean_sql_query(self, sql_query: str) -> str:
        """
        Clean and format the SQL query.
        
        Args:
            sql_query: Raw SQL query from LLM
            
        Returns:
            Cleaned SQL query
        """
        # Remove markdown code blocks if present
        sql_query = re.sub(r'^```sql\n', '', sql_query)
        sql_query = re.sub(r'^```\n', '', sql_query)
        sql_query = re.sub(r'\n```$', '', sql_query)
        sql_query = re.sub(r'```$', '', sql_query)
        
        # Remove leading/trailing whitespace
        sql_query = sql_query.strip()
        
        # Format the SQL for better readability
        # Add newlines before major clauses
        keywords = ['SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'LIMIT', 'JOIN', 'LEFT JOIN', 'INNER JOIN']
        for keyword in keywords:
            sql_query = re.sub(f'\\s+{keyword}', f'\n{keyword}', sql_query, flags=re.IGNORECASE)
        
        # Remove multiple spaces
        sql_query = re.sub(r'\s+', ' ', sql_query)
        
        # Ensure single line breaks where we added them
        sql_query = re.sub(r'\n\s*\n', '\n', sql_query)
        
        return sql_query.strip()
    
    def _validate_sql(self, sql_query: str) -> Dict[str, Any]:
        """
        Validate the SQL query by attempting to explain it.
        
        Args:
            sql_query: SQL query to validate
            
        Returns:
            Dictionary with validation results
        """
        # Check for dangerous operations
        dangerous_keywords = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE', 'REPLACE', 'INSERT', 'UPDATE']
        query_upper = sql_query.upper()
        
        for keyword in dangerous_keywords:
            if keyword in query_upper:
                return {
                    'is_valid': False,
                    'error': f'Dangerous operation detected: {keyword}'
                }
        
        # Check for multiple statements (SQL injection prevention)
        if ';' in sql_query.strip()[:-1]:  # Allow semicolon at the end
            return {
                'is_valid': False,
                'error': 'Multiple SQL statements detected'
            }
        
        try:
            cursor = self.conn.cursor()
            # Use EXPLAIN QUERY PLAN to validate without executing
            cursor.execute(f"EXPLAIN QUERY PLAN {sql_query}")
            return {'is_valid': True}
        except sqlite3.Error as e:
            return {'is_valid': False, 'error': str(e)}
    
    def execute_query(self, sql_query: str) -> Dict[str, Any]:
        """
        Execute the SQL query and return results.
        
        Args:
            sql_query: SQL query to execute
            
        Returns:
            Dictionary with query results or error information
        """
        start_time = time.time()
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql_query)
            
            # Get column names
            columns = [description[0] for description in cursor.description] if cursor.description else []
            
            # Fetch results
            results = cursor.fetchall()
            
            execution_time = time.time() - start_time
            
            logger.info(f"Query executed successfully, returned {len(results)} rows in {execution_time:.2f}s")
            
            return {
                'success': True,
                'columns': columns,
                'data': results,
                'row_count': len(results),
                'execution_time': execution_time
            }
            
        except sqlite3.Error as e:
            logger.error(f"Query execution failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'columns': [],
                'data': [],
                'execution_time': time.time() - start_time
            }
    
    def explain_query(self, sql_query: str) -> str:
        """
        Generate a natural language explanation of what the SQL query does.
        
        Args:
            sql_query: SQL query to explain
            
        Returns:
            Natural language explanation
        """
        prompt = f"""Explain this SQL query in simple terms that a non-technical person can understand:

{sql_query}

Provide a brief, clear explanation of what data this query retrieves. Be specific about what tables are being used and what the result will show."""

        try:
            response = self.client.chat.completions.create(
                model="gpt-3.5-turbo",  # Use cheaper model for explanations
                messages=[
                    {"role": "system", "content": "You are a helpful assistant that explains SQL queries in simple terms."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=200
            )
            
            return response.choices[0].message.content.strip()
            
        except Exception as e:
            logger.error(f"Failed to generate explanation: {e}")
            return f"Could not generate explanation: {str(e)}"
    
    def process_question(self, question: str) -> Dict[str, Any]:
        """
        Full pipeline: question → SQL → execution → results.
        
        Args:
            question: Natural language question
            
        Returns:
            Dictionary with complete results including data
        """
        print(f"\n🤔 Processing: {question}")
        print("-" * 50)
        
        # Generate SQL
        sql_result = self.generate_sql(question)
        
        if not sql_result['success']:
            return {
                'success': False,
                'error': f"Failed to generate SQL: {sql_result['error']}",
                'question': question
            }
        
        sql_query = sql_result['sql_query']
        print(f"\n📝 Generated SQL:\n{sql_query}")
        
        # Execute query
        execution_result = self.execute_query(sql_query)
        
        if not execution_result['success']:
            return {
                'success': False,
                'error': f"Query execution failed: {execution_result['error']}",
                'question': question,
                'sql': sql_query
            }
        
        # Get explanation
        explanation = self.explain_query(sql_query)
        
        # Store in conversation history
        self.conversation_history.append({
            'question': question,
            'sql': sql_query,
            'row_count': execution_result['row_count'],
            'timestamp': datetime.now().isoformat()
        })
        
        # Trim history if it gets too long
        if len(self.conversation_history) > 10:
            self.conversation_history = self.conversation_history[-10:]
        
        return {
            'success': True,
            'question': question,
            'sql': sql_query,
            'explanation': explanation,
            'columns': execution_result['columns'],
            'data': execution_result['data'],
            'row_count': execution_result['row_count'],
            'execution_time': execution_result['execution_time'],
            'generation_time': sql_result['generation_time'],
            'from_cache': sql_result.get('from_cache', False)
        }
    
    def format_results(self, result: Dict[str, Any]) -> str:
        """
        Format query results for display.
        
        Args:
            result: Dictionary with query results
            
        Returns:
            Formatted string for display
        """
        if not result['success']:
            return f"❌ Error: {result.get('error', 'Unknown error')}"
        
        output = []
        
        # Add explanation
        output.append(f"\n✅ Query Explanation: {result['explanation']}")
        
        # Add performance metrics
        cache_indicator = " (from cache)" if result.get('from_cache') else ""
        output.append(f"\n⏱️ Performance:")
        output.append(f"  - SQL Generation: {result.get('generation_time', 0):.2f}s{cache_indicator}")
        output.append(f"  - Query Execution: {result.get('execution_time', 0):.2f}s")
        
        # Add results
        output.append(f"\n📊 Results ({result['row_count']} rows):\n")
        
        if result['data']:
            # Use tabulate for nice formatting
            table = tabulate(
                result['data'],
                headers=result['columns'],
                tablefmt='grid',
                floatfmt='.2f',
                maxcolwidths=30  # Limit column width for readability
            )
            output.append(table)
        else:
            output.append("No results found.")
        
        return '\n'.join(output)
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get agent usage statistics.
        
        Returns:
            Dictionary with statistics
        """
        return {
            'total_queries': len(self.conversation_history),
            'cache_size': len(self.query_cache),
            'history_size': len(self.conversation_history),
            'database': self.db_path,
            'model': self.model
        }
    
    def clear_cache(self):
        """Clear the query cache."""
        self.query_cache.clear()
        logger.info("Query cache cleared")
    
    def clear_history(self):
        """Clear conversation history."""
        self.conversation_history.clear()
        logger.info("Conversation history cleared")
    
    def _get_cache_key(self, query: str) -> str:
        """
        Generate a cache key for a query.
        
        Args:
            query: Natural language query
            
        Returns:
            Cache key string
        """
        # Simple normalization for cache key
        return query.lower().strip()
    
    def close(self):
        """Clean up database connection and resources."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
        
        # Save cache statistics
        logger.info(f"Session statistics: {self.get_statistics()}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()