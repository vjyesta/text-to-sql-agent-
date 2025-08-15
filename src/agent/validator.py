"""
Query validation module.
Validates SQL queries for safety, correctness, and performance.
"""

import re
import sqlite3
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)


class QueryValidator:
    """
    Validates SQL queries for safety and correctness.
    
    This class performs multiple validation checks to ensure queries
    are safe to execute and will produce expected results.
    """
    
    def __init__(self, db_connection: Optional[sqlite3.Connection] = None):
        """
        Initialize the query validator.
        
        Args:
            db_connection: Optional database connection for schema validation
        """
        self.conn = db_connection
        
        # Dangerous keywords that should not be in queries
        self.dangerous_keywords = [
            'DROP', 'DELETE', 'TRUNCATE', 'ALTER', 
            'CREATE', 'REPLACE', 'INSERT', 'UPDATE',
            'GRANT', 'REVOKE', 'EXEC', 'EXECUTE',
            'SHUTDOWN', 'RENAME', 'ATTACH', 'DETACH'
        ]
        
        # Suspicious patterns that might indicate SQL injection
        self.suspicious_patterns = [
            r';\s*DROP',  # Attempt to drop tables
            r'--\s*$',    # SQL comment at end (might hide malicious code)
            r'\/\*.*\*\/', # Block comments (could hide code)
            r'xp_\w+',    # SQL Server extended procedures
            r'sp_\w+',    # SQL Server stored procedures
            r'0x[0-9a-fA-F]+',  # Hex encoding
            r'CHAR\s*\(',  # Character encoding
            r'INTO\s+OUTFILE',  # File output
            r'LOAD_FILE',  # File loading
        ]
        
        # Valid query patterns
        self.valid_patterns = {
            'select': r'^\s*SELECT\s+',
            'with': r'^\s*WITH\s+',  # CTE queries
            'explain': r'^\s*EXPLAIN\s+',  # Query plans
        }
        
        # Performance warning thresholds
        self.performance_thresholds = {
            'max_joins': 5,
            'max_subqueries': 3,
            'max_union': 3,
            'max_wildcards': 2,
            'recommended_limit': 1000
        }
        
        # Validation statistics
        self.stats = {
            'queries_validated': 0,
            'queries_passed': 0,
            'queries_failed': 0,
            'security_violations': 0,
            'syntax_errors': 0,
            'performance_warnings': 0
        }
        
        logger.info("QueryValidator initialized")
    
    def validate(self, sql_query: str, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Perform complete validation of a SQL query.
        
        Args:
            sql_query: SQL query to validate
            context: Optional context information
            
        Returns:
            Validation result dictionary
        """
        self.stats['queries_validated'] += 1
        
        result = {
            'is_valid': True,
            'query': sql_query,
            'errors': [],
            'warnings': [],
            'security_issues': [],
            'syntax_issues': [],
            'performance_issues': [],
            'suggestions': [],
            'risk_level': 'low',  # low, medium, high
            'timestamp': datetime.now().isoformat()
        }
        
        # Run all validation checks
        checks = [
            ('security', self._check_security),
            ('syntax', self._check_syntax),
            ('structure', self._check_structure),
            ('performance', self._check_performance),
            ('schema', self._check_schema),
            ('logic', self._check_logic),
            ('best_practices', self._check_best_practices)
        ]
        
        for check_name, check_func in checks:
            try:
                check_result = check_func(sql_query, context)
                
                # Merge results
                if not check_result['passed']:
                    result['is_valid'] = False
                
                result['errors'].extend(check_result.get('errors', []))
                result['warnings'].extend(check_result.get('warnings', []))
                result['suggestions'].extend(check_result.get('suggestions', []))
                
                # Specific issue tracking
                if check_name == 'security':
                    result['security_issues'] = check_result.get('issues', [])
                    if check_result.get('issues'):
                        self.stats['security_violations'] += 1
                elif check_name == 'syntax':
                    result['syntax_issues'] = check_result.get('issues', [])
                    if check_result.get('issues'):
                        self.stats['syntax_errors'] += 1
                elif check_name == 'performance':
                    result['performance_issues'] = check_result.get('issues', [])
                    if check_result.get('issues'):
                        self.stats['performance_warnings'] += 1
                        
            except Exception as e:
                logger.error(f"Validation check {check_name} failed: {e}")
                result['warnings'].append(f"Could not complete {check_name} check")
        
        # Calculate risk level
        result['risk_level'] = self._calculate_risk_level(result)
        
        # Update statistics
        if result['is_valid']:
            self.stats['queries_passed'] += 1
        else:
            self.stats['queries_failed'] += 1
        
        # Add summary
        result['summary'] = self._generate_summary(result)
        
        return result
    
    def _check_structure(self, query: str, context: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Check query structure and composition.
        
        Args:
            query: SQL query
            context: Optional context
            
        Returns:
            Structure check result
        """
        result = {
            'passed': True,
            'errors': [],
            'warnings': [],
            'issues': [],
            'suggestions': []
        }
        
        # Check clause order
        clause_order = self._check_clause_order(query)
        if not clause_order['valid']:
            result['warnings'].append(f"Incorrect clause order: {clause_order['message']}")
        
        # Check for required clauses
        if 'GROUP BY' in query.upper() and 'SELECT' in query.upper():
            if not self._check_group_by_validity(query):
                result['warnings'].append("GROUP BY clause may have issues")
                result['suggestions'].append("Ensure all non-aggregate SELECT columns are in GROUP BY")
        
        # Check JOIN conditions
        if 'JOIN' in query.upper():
            join_issues = self._check_join_conditions(query)
            if join_issues:
                result['warnings'].extend(join_issues)
        
        # Check subquery structure
        subquery_count = query.upper().count('SELECT') - 1
        if subquery_count > self.performance_thresholds['max_subqueries']:
            result['warnings'].append(f"Too many subqueries ({subquery_count})")
            result['suggestions'].append("Consider using JOINs or CTEs instead of nested subqueries")
        
        return result
    
    def _check_performance(self, query: str, context: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Check for potential performance issues.
        
        Args:
            query: SQL query
            context: Optional context
            
        Returns:
            Performance check result
        """
        result = {
            'passed': True,
            'errors': [],
            'warnings': [],
            'issues': [],
            'suggestions': []
        }
        
        query_upper = query.upper()
        
        # Check for SELECT *
        wildcard_count = query.count('*')
        if wildcard_count > self.performance_thresholds['max_wildcards']:
            result['warnings'].append(f"Too many wildcards ({wildcard_count})")
            result['issues'].append({
                'type': 'excessive_wildcards',
                'count': wildcard_count,
                'severity': 'medium'
            })
            result['suggestions'].append("Specify exact columns instead of using SELECT *")
        
        # Check for missing LIMIT
        if 'LIMIT' not in query_upper and 'COUNT(' not in query_upper:
            result['warnings'].append("No LIMIT clause found")
            result['suggestions'].append(f"Consider adding LIMIT {self.performance_thresholds['recommended_limit']}")
        
        # Check for too many JOINs
        join_count = query_upper.count('JOIN')
        if join_count > self.performance_thresholds['max_joins']:
            result['warnings'].append(f"Too many JOINs ({join_count})")
            result['issues'].append({
                'type': 'excessive_joins',
                'count': join_count,
                'severity': 'medium'
            })
            result['suggestions'].append("Consider breaking the query into smaller parts")
        
        # Check for cartesian product risk
        if self._has_cartesian_product_risk(query):
            result['warnings'].append("Potential cartesian product detected")
            result['issues'].append({
                'type': 'cartesian_product',
                'severity': 'high'
            })
            result['suggestions'].append("Ensure proper JOIN conditions are specified")
        
        # Check for functions in WHERE clause
        if self._has_functions_in_where(query):
            result['warnings'].append("Functions in WHERE clause may prevent index usage")
            result['suggestions'].append("Consider restructuring to allow index usage")
        
        # Check for OR conditions (can be slow)
        or_count = query_upper.count(' OR ')
        if or_count > 5:
            result['warnings'].append(f"Many OR conditions ({or_count}) may impact performance")
            result['suggestions'].append("Consider using IN clause or UNION instead")
        
        # Check for LIKE with leading wildcard
        if re.search(r"LIKE\s+['\"]%", query, re.IGNORECASE):
            result['warnings'].append("LIKE with leading wildcard prevents index usage")
            result['suggestions'].append("Consider full-text search or restructuring the query")
        
        return result
    
    def _check_schema(self, query: str, context: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Validate query against database schema.
        
        Args:
            query: SQL query
            context: Optional context with schema information
            
        Returns:
            Schema validation result
        """
        result = {
            'passed': True,
            'errors': [],
            'warnings': [],
            'issues': [],
            'suggestions': []
        }
        
        if not self.conn and not context:
            result['warnings'].append("Schema validation skipped (no connection or context)")
            return result
        
        # Extract table names from query
        tables = self._extract_table_names(query)
        
        # Validate tables exist
        if self.conn:
            existing_tables = self._get_existing_tables()
            for table in tables:
                if table.lower() not in [t.lower() for t in existing_tables]:
                    result['errors'].append(f"Table '{table}' does not exist")
                    result['passed'] = False
                    result['issues'].append({
                        'type': 'missing_table',
                        'table': table,
                        'severity': 'critical'
                    })
        
        # Extract column references
        columns = self._extract_column_references(query)
        
        # Validate columns exist (if we have schema info)
        if context and 'schema' in context:
            for table, column in columns:
                if table in context['schema']:
                    table_columns = context['schema'][table].get('columns', [])
                    if column not in table_columns:
                        result['warnings'].append(f"Column '{column}' may not exist in table '{table}'")
                        result['suggestions'].append(f"Verify column '{column}' exists in '{table}'")
        
        return result
    
    def _check_logic(self, query: str, context: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Check for logical issues in the query.
        
        Args:
            query: SQL query
            context: Optional context
            
        Returns:
            Logic check result
        """
        result = {
            'passed': True,
            'errors': [],
            'warnings': [],
            'issues': [],
            'suggestions': []
        }
        
        # Check for always true/false conditions
        if re.search(r'WHERE\s+1\s*=\s*1', query, re.IGNORECASE):
            result['warnings'].append("Always-true condition detected")
        
        if re.search(r'WHERE\s+1\s*=\s*0', query, re.IGNORECASE):
            result['warnings'].append("Always-false condition detected")
        
        # Check for NULL comparisons
        if re.search(r'=\s*NULL', query, re.IGNORECASE):
            result['warnings'].append("Use IS NULL instead of = NULL")
            result['suggestions'].append("NULL comparisons should use IS NULL or IS NOT NULL")
        
        # Check for date string comparisons
        if re.search(r"WHERE\s+\w+\s*[<>=]+\s*'\d{4}-\d{2}-\d{2}'", query, re.IGNORECASE):
            result['suggestions'].append("Consider using DATE() function for date comparisons")
        
        # Check for duplicate conditions
        duplicate_conditions = self._find_duplicate_conditions(query)
        if duplicate_conditions:
            result['warnings'].append("Duplicate conditions found")
            result['suggestions'].append("Remove redundant conditions")
        
        # Check for contradictory conditions
        if self._has_contradictory_conditions(query):
            result['errors'].append("Contradictory conditions detected")
            result['passed'] = False
        
        return result
    
    def _check_best_practices(self, query: str, context: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Check adherence to SQL best practices.
        
        Args:
            query: SQL query
            context: Optional context
            
        Returns:
            Best practices check result
        """
        result = {
            'passed': True,
            'errors': [],
            'warnings': [],
            'issues': [],
            'suggestions': []
        }
        
        # Check for table aliases
        if 'JOIN' in query.upper() and not re.search(r'FROM\s+\w+\s+(?:AS\s+)?\w{1,3}\s', query, re.IGNORECASE):
            result['suggestions'].append("Consider using table aliases for better readability")
        
        # Check for column aliases in complex expressions
        if re.search(r'SELECT.*\(.*\).*FROM', query, re.IGNORECASE):
            if not re.search(r'AS\s+\w+', query, re.IGNORECASE):
                result['suggestions'].append("Consider using column aliases for complex expressions")
        
        # Check for DISTINCT usage
        if 'DISTINCT' in query.upper():
            result['suggestions'].append("Verify if DISTINCT is necessary (it can impact performance)")
        
        # Check for UNION vs UNION ALL
        if 'UNION' in query.upper() and 'UNION ALL' not in query.upper():
            result['suggestions'].append("Consider UNION ALL instead of UNION if duplicates are acceptable")
        
        # Check for explicit column ordering
        if 'ORDER BY' in query.upper():
            if re.search(r'ORDER\s+BY\s+\d+', query, re.IGNORECASE):
                result['warnings'].append("Ordering by column position is fragile")
                result['suggestions'].append("Use column names instead of positions in ORDER BY")
        
        return result
    
    # Helper methods
    
    def _has_multiple_statements(self, query: str) -> bool:
        """Check if query contains multiple statements."""
        # Remove quoted semicolons
        query_cleaned = re.sub(r"'[^']*'", "", query)
        query_cleaned = re.sub(r'"[^"]*"', "", query_cleaned)
        
        # Count semicolons (except at the end)
        semicolons = query_cleaned.rstrip(';').count(';')
        return semicolons > 0
    
    def _has_unescaped_input(self, query: str) -> bool:
        """Check for potential unescaped user input."""
        # Look for concatenation patterns that might indicate dynamic SQL
        patterns = [
            r'\+\s*@',  # String concatenation with variables
            r'\|\|',    # SQL concatenation operator
            r'CONCAT\s*\(',  # CONCAT function
        ]
        
        for pattern in patterns:
            if re.search(pattern, query, re.IGNORECASE):
                return True
        return False
    
    def _is_read_only(self, query: str) -> bool:
        """Check if query is read-only."""
        query_start = query.strip().upper()
        read_only_starts = ['SELECT', 'WITH', 'EXPLAIN']
        
        for start in read_only_starts:
            if query_start.startswith(start):
                return True
        return False
    
    def _check_balanced_parentheses(self, query: str) -> bool:
        """Check if parentheses are balanced."""
        count = 0
        for char in query:
            if char == '(':
                count += 1
            elif char == ')':
                count -= 1
            if count < 0:
                return False
        return count == 0
    
    def _check_balanced_quotes(self, query: str) -> bool:
        """Check if quotes are balanced."""
        single_quotes = query.count("'") % 2 == 0
        double_quotes = query.count('"') % 2 == 0
        return single_quotes and double_quotes
    
    def _check_select_structure(self, query: str) -> bool:
        """Check if SELECT statement has proper structure."""
        if not query.strip().upper().startswith(('SELECT', 'WITH', 'EXPLAIN')):
            return True  # Not a SELECT query
        
        # Basic structure check
        required_parts = ['SELECT', 'FROM']
        query_upper = query.upper()
        
        for part in required_parts:
            if part not in query_upper:
                return False
        
        return True
    
    def _validate_with_database(self, query: str) -> Dict[str, Any]:
        """Validate query with actual database."""
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"EXPLAIN QUERY PLAN {query}")
            return {'valid': True}
        except sqlite3.Error as e:
            return {'valid': False, 'error': str(e)}
    
    def _check_common_syntax_mistakes(self, query: str) -> List[str]:
        """Check for common SQL syntax mistakes."""
        mistakes = []
        
        # Missing commas between columns
        if re.search(r'SELECT\s+\w+\s+\w+\s+FROM', query, re.IGNORECASE):
            mistakes.append("Possible missing comma between SELECT columns")
        
        # Wrong JOIN syntax
        if re.search(r'JOIN\s+\w+\s+WHERE', query, re.IGNORECASE):
            mistakes.append("JOIN without ON clause")
        
        return mistakes
    
    def _check_clause_order(self, query: str) -> Dict[str, Any]:
        """Check if SQL clauses are in correct order."""
        correct_order = ['WITH', 'SELECT', 'FROM', 'JOIN', 'WHERE', 'GROUP BY', 'HAVING', 'ORDER BY', 'LIMIT']
        
        # Find positions of each clause
        positions = {}
        for clause in correct_order:
            match = re.search(r'\b' + clause + r'\b', query, re.IGNORECASE)
            if match:
                positions[clause] = match.start()
        
        # Check order
        last_pos = -1
        for clause in correct_order:
            if clause in positions:
                if positions[clause] < last_pos:
                    return {'valid': False, 'message': f"{clause} appears in wrong position"}
                last_pos = positions[clause]
        
        return {'valid': True}
    
    def _check_group_by_validity(self, query: str) -> bool:
        """Check if GROUP BY clause is valid."""
        # This is a simplified check
        # Full validation would require SQL parsing
        return 'GROUP BY' in query.upper()
    
    def _check_join_conditions(self, query: str) -> List[str]:
        """Check for issues with JOIN conditions."""
        issues = []
        
        # Check for JOIN without ON
        if re.search(r'JOIN\s+\w+\s+(?:JOIN|WHERE|GROUP|ORDER|$)', query, re.IGNORECASE):
            issues.append("JOIN may be missing ON clause")
        
        # Check for multiple conditions without proper operators
        if 'ON' in query.upper():
            on_clause = re.search(r'ON\s+([^WHERE|GROUP|ORDER|JOIN]+)', query, re.IGNORECASE)
            if on_clause:
                conditions = on_clause.group(1)
                if 'AND' not in conditions.upper() and 'OR' not in conditions.upper() and '=' in conditions:
                    if conditions.count('=') > 1:
                        issues.append("Multiple JOIN conditions may need AND/OR operators")
        
        return issues
    
    def _has_cartesian_product_risk(self, query: str) -> bool:
        """Check if query might produce cartesian product."""
        # Multiple tables in FROM without JOIN
        from_match = re.search(r'FROM\s+([\w\s,]+?)(?:WHERE|GROUP|ORDER|LIMIT|$)', query, re.IGNORECASE)
        if from_match:
            tables = from_match.group(1).split(',')
            if len(tables) > 1 and 'JOIN' not in query.upper():
                # Check if WHERE clause has join conditions
                if 'WHERE' not in query.upper():
                    return True
        return False
    
    def _has_functions_in_where(self, query: str) -> bool:
        """Check if WHERE clause contains functions on columns."""
        patterns = [
            r'WHERE.*\b(DATE|YEAR|MONTH|DAY|UPPER|LOWER|TRIM|LENGTH)\s*\(',
            r'WHERE.*\b\w+\s*\([^)]*\w+\.[^)]*\)'
        ]
        
        for pattern in patterns:
            if re.search(pattern, query, re.IGNORECASE):
                return True
        return False
    
    def _extract_table_names(self, query: str) -> List[str]:
        """Extract table names from query."""
        tables = []
        
        # FROM clause
        from_match = re.findall(r'FROM\s+(\w+)', query, re.IGNORECASE)
        tables.extend(from_match)
        
        # JOIN clauses
        join_matches = re.findall(r'JOIN\s+(\w+)', query, re.IGNORECASE)
        tables.extend(join_matches)
        
        return list(set(tables))
    
    def _extract_column_references(self, query: str) -> List[Tuple[str, str]]:
        """Extract column references as (table, column) tuples."""
        references = []
        
        # Pattern for table.column
        pattern = r'(\w+)\.(\w+)'
        matches = re.findall(pattern, query)
        
        for match in matches:
            references.append(match)
        
        return references
    
    def _get_existing_tables(self) -> List[str]:
        """Get list of existing tables from database."""
        if not self.conn:
            return []
        
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return [row[0] for row in cursor.fetchall()]
    
    def _find_duplicate_conditions(self, query: str) -> bool:
        """Find duplicate conditions in WHERE clause."""
        where_match = re.search(r'WHERE\s+(.*?)(?:GROUP|ORDER|LIMIT|$)', query, re.IGNORECASE)
        if where_match:
            conditions = where_match.group(1)
            # Simple check for repeated patterns
            parts = re.split(r'\s+AND\s+|\s+OR\s+', conditions, flags=re.IGNORECASE)
            return len(parts) != len(set(parts))
        return False
    
    def _has_contradictory_conditions(self, query: str) -> bool:
        """Check for contradictory conditions."""
        # Look for patterns like: WHERE x = 1 AND x = 2
        pattern = r'(\w+)\s*=\s*([^\s]+).*AND.*\1\s*=\s*([^\s]+)'
        match = re.search(pattern, query, re.IGNORECASE)
        if match:
            value1 = match.group(2)
            value2 = match.group(3)
            if value1 != value2:
                return True
        return False
    
    def _calculate_risk_level(self, result: Dict[str, Any]) -> str:
        """Calculate overall risk level."""
        if result['security_issues']:
            return 'high'
        elif result['errors']:
            return 'high'
        elif len(result['warnings']) > 3:
            return 'medium'
        else:
            return 'low'
    
    def _generate_summary(self, result: Dict[str, Any]) -> str:
        """Generate a summary of validation results."""
        if result['is_valid']:
            if not result['warnings']:
                return "Query validated successfully with no issues"
            else:
                return f"Query validated with {len(result['warnings'])} warnings"
        else:
            return f"Query validation failed: {len(result['errors'])} errors, {len(result['warnings'])} warnings"
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get validation statistics."""
        pass_rate = 0
        if self.stats['queries_validated'] > 0:
            pass_rate = (self.stats['queries_passed'] / self.stats['queries_validated']) * 100
        
        return {
            'queries_validated': self.stats['queries_validated'],
            'queries_passed': self.stats['queries_passed'],
            'queries_failed': self.stats['queries_failed'],
            'pass_rate': f"{pass_rate:.1f}%",
            'security_violations': self.stats['security_violations'],
            'syntax_errors': self.stats['syntax_errors'],
            'performance_warnings': self.stats['performance_warnings']
        }
    
    def reset_statistics(self):
        """Reset validation statistics."""
        self.stats = {
            'queries_validated': 0,
            'queries_passed': 0,
            'queries_failed': 0,
            'security_violations': 0,
            'syntax_errors': 0,
            'performance_warnings': 0
        }
        logger.info("Validation statistics reset")
        
    def _check_security(self, query: str, context: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Check for security issues in the query.
        
        Args:
            query: SQL query
            context: Optional context
            
        Returns:
            Security check result
        """
        result = {
            'passed': True,
            'errors': [],
            'warnings': [],
            'issues': [],
            'suggestions': []
        }
        
        query_upper = query.upper()
        
        # Check for dangerous keywords
        for keyword in self.dangerous_keywords:
            if keyword in query_upper:
                result['passed'] = False
                result['errors'].append(f"Dangerous operation detected: {keyword}")
                result['issues'].append({
                    'type': 'dangerous_keyword',
                    'keyword': keyword,
                    'severity': 'critical'
                })
        
        # Check for suspicious patterns
        for pattern in self.suspicious_patterns:
            if re.search(pattern, query, re.IGNORECASE):
                result['passed'] = False
                result['errors'].append(f"Suspicious pattern detected: {pattern}")
                result['issues'].append({
                    'type': 'suspicious_pattern',
                    'pattern': pattern,
                    'severity': 'high'
                })
        
        # Check for multiple statements (SQL injection risk)
        if self._has_multiple_statements(query):
            result['passed'] = False
            result['errors'].append("Multiple SQL statements detected")
            result['issues'].append({
                'type': 'multiple_statements',
                'severity': 'high'
            })
        
        # Check for unescaped user input patterns
        if self._has_unescaped_input(query):
            result['warnings'].append("Potential unescaped user input detected")
            result['suggestions'].append("Ensure all user inputs are properly parameterized")
        
        # Check query type
        if not self._is_read_only(query):
            result['warnings'].append("Query performs write operations")
        
        return result
    
    def _check_syntax(self, query: str, context: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Check SQL syntax validity.
        
        Args:
            query: SQL query
            context: Optional context
            
        Returns:
            Syntax check result
        """
        result = {
            'passed': True,
            'errors': [],
            'warnings': [],
            'issues': [],
            'suggestions': []
        }
        
        # Check for balanced parentheses
        if not self._check_balanced_parentheses(query):
            result['passed'] = False
            result['errors'].append("Unbalanced parentheses")
            result['issues'].append({
                'type': 'unbalanced_parentheses',
                'severity': 'high'
            })
        
        # Check for balanced quotes
        if not self._check_balanced_quotes(query):
            result['passed'] = False
            result['errors'].append("Unbalanced quotes")
            result['issues'].append({
                'type': 'unbalanced_quotes',
                'severity': 'high'
            })
        
        # Check basic SELECT structure
        if not self._check_select_structure(query):
            result['warnings'].append("Unusual SELECT statement structure")
        
        # Validate with actual database if connection available
        if self.conn:
            validation = self._validate_with_database(query)
            if not validation['valid']:
                result['passed'] = False
                result['errors'].append(f"SQL syntax error: {validation['error']}")
                result['issues'].append({
                    'type': 'syntax_error',
                    'error': validation['error'],
                    'severity': 'critical'
                })
        
        # Check for common syntax mistakes
        common_mistakes = self._check_common_syntax_mistakes(query)
        if common_mistakes:
            result['warnings'].extend(common_mistakes)
            result['suggestions'].append("Review common SQL syntax patterns")
        
        return result