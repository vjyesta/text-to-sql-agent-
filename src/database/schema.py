import sqlite3
import json

def extract_schema_info():
    """
    Extracts detailed schema information for the agent to understand
    """
    conn = sqlite3.connect('ecommerce.db')
    cursor = conn.cursor()
    
    # Get all table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    
    schema_info = {}
    
    for table in tables:
        table_name = table[0]
        
        # Get column information
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        
        # Get foreign key information
        cursor.execute(f"PRAGMA foreign_key_list({table_name})")
        foreign_keys = cursor.fetchall()
        
        schema_info[table_name] = {
            'columns': [],
            'foreign_keys': [],
            'sample_data': []
        }
        
        # Process columns
        for col in columns:
            schema_info[table_name]['columns'].append({
                'name': col[1],
                'type': col[2],
                'nullable': not col[3],
                'primary_key': bool(col[5])
            })
        
        # Process foreign keys
        for fk in foreign_keys:
            schema_info[table_name]['foreign_keys'].append({
                'column': fk[3],
                'references_table': fk[2],
                'references_column': fk[4]
            })
        
        # Get sample data (3 rows)
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
        sample_rows = cursor.fetchall()
        column_names = [col[0] for col in cursor.description]
        
        for row in sample_rows:
            schema_info[table_name]['sample_data'].append(
                dict(zip(column_names, row))
            )
    
    conn.close()
    
    # Save schema information to JSON file
    with open('schema_info.json', 'w') as f:
        json.dump(schema_info, f, indent=2, default=str)
    
    print("Schema information saved to 'schema_info.json'")
    return schema_info

def generate_schema_description():
    """
    Generates a human-readable schema description for the LLM
    """
    schema_info = extract_schema_info()
    
    description = "E-COMMERCE DATABASE SCHEMA:\n\n"
    
    for table_name, info in schema_info.items():
        description += f"TABLE: {table_name}\n"
        description += "Columns:\n"
        
        for col in info['columns']:
            pk = " (PRIMARY KEY)" if col['primary_key'] else ""
            nullable = " (NULLABLE)" if col['nullable'] else " (NOT NULL)"
            description += f"  - {col['name']}: {col['type']}{pk}{nullable}\n"
        
        if info['foreign_keys']:
            description += "Foreign Keys:\n"
            for fk in info['foreign_keys']:
                description += f"  - {fk['column']} → {fk['references_table']}.{fk['references_column']}\n"
        
        description += "\n"
    
    # Save to text file
    with open('schema_description.txt', 'w') as f:
        f.write(description)
    
    print("Schema description saved to 'schema_description.txt'")
    return description

if __name__ == "__main__":
    generate_schema_description()