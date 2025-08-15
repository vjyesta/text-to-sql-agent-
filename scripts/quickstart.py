#!/usr/bin/env python3
"""
Quick setup script for the Text-to-SQL Agent project.
This script handles all initialization tasks.
"""

import os
import sys
import subprocess
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))


class ProjectSetup:
    """
    Handles complete project setup and initialization.
    """
    
    def __init__(self):
        """Initialize setup with project paths."""
        self.project_root = Path(__file__).parent.parent
        self.data_dir = self.project_root / 'data'
        self.venv_dir = self.project_root / 'venv'

    
    def check_virtual_environment(self) -> bool:
        """
        Check if we're running in a virtual environment.
        
        Returns:
            True if in virtual environment, False otherwise
        """
        return hasattr(sys, 'real_prefix') or (
            hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix
        )
    
    def create_directories(self):
        """Create necessary project directories."""
        directories = [
            self.data_dir,
            self.project_root / 'logs',
            self.project_root / 'cache'
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
            print(f"✅ Created directory: {directory}")
    
    def setup_environment(self):
        """Set up environment variables."""
        env_file = self.project_root / '.env'
        env_example = self.project_root / '.env.example'
        
        if not env_file.exists() and env_example.exists():
            print("\n🔑 Setting up environment variables...")
            
            # Read example file
            with open(env_example, 'r') as f:
                content = f.read()
            
            # Get API key from user
            api_key = input("Enter your OpenAI API key: ").strip()
            
            # Replace placeholder with actual key
            content = content.replace('your-openai-api-key-here', api_key)
            
            # Write to .env file
            with open(env_file, 'w') as f:
                f.write(content)
            
            print("✅ Environment file created")
    
    def initialize_database(self):
        """Create and populate the database."""
        print("\n🗄️ Initializing database...")
        
        from src.database.creator import DatabaseCreator
        from src.database.seeder import DatabaseSeeder
        
        # Create database
        creator = DatabaseCreator(str(self.data_dir / 'ecommerce.db'))
        conn = creator.create_database()
        
        # Populate with sample data
        seeder = DatabaseSeeder(conn)
        seeder.populate_all()
        
        print("✅ Database initialized with sample data")
    
    def run_setup(self):
        """Run complete setup process."""
        print("🚀 TEXT-TO-SQL AGENT - PROJECT SETUP")
        print("=" * 50)
        
        # Check virtual environment
        if not self.check_virtual_environment():
            print("⚠️  Warning: Not running in a virtual environment!")
            print("   It's recommended to use a virtual environment.")
            continue_anyway = input("Continue anyway? (y/n): ").lower()
            if continue_anyway != 'y':
                sys.exit(0)
        
        # Create directories
        print("\n📁 Creating project directories...")
        self.create_directories()
        
        # Set up environment
        self.setup_environment()
        
        # Initialize database
        self.initialize_database()
        
        print("\n" + "=" * 50)
        print("✨ Setup complete!")
        print("\nTo start the agent, run:")
        print("  python scripts/run_agent.py")


if __name__ == "__main__":
    setup = ProjectSetup()
    setup.run_setup()