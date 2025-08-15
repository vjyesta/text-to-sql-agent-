"""
Database creation module.
Handles the creation of the e-commerce database schema with all necessary tables,
relationships, and indexes for optimal query performance.
"""

import sqlite3
import logging
import os
from typing import Optional, List, Tuple
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseCreator:
    """
    Handles creation and setup of the e-commerce database.
    
    This class is responsible for creating all tables, indexes,
    and relationships needed for our e-commerce system.
    """
    
    def __init__(self, db_path: str = 'data/ecommerce.db'):
        """
        Initialize the database creator.
        
        Args:
            db_path: Path where the database will be created
        """
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        
        # Ensure the directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        logger.info(f"DatabaseCreator initialized with path: {db_path}")
    
    def create_database(self, drop_existing: bool = True) -> sqlite3.Connection:
        """
        Create the complete database schema.
        
        This method creates all tables in the correct order to respect
        foreign key constraints.
        
        Args:
            drop_existing: Whether to drop existing tables before creating new ones
            
        Returns:
            Connection to the created database
        """
        logger.info(f"Creating database at {self.db_path}")
        
        try:
            # Establish connection
            self.conn = sqlite3.connect(self.db_path)
            cursor = self.conn.cursor()
            
            # Enable foreign keys for referential integrity
            cursor.execute("PRAGMA foreign_keys = ON")
            
            # Set journal mode for better concurrency
            cursor.execute("PRAGMA journal_mode = WAL")
            
            # Drop existing tables if requested
            if drop_existing:
                self._drop_existing_tables(cursor)
            
            # Create tables in order of dependencies
            self._create_categories_table(cursor)
            self._create_customers_table(cursor)
            self._create_products_table(cursor)
            self._create_orders_table(cursor)
            self._create_order_items_table(cursor)
            
            # Create additional utility tables
            self._create_inventory_log_table(cursor)
            self._create_product_reviews_table(cursor)
            self._create_cart_table(cursor)
            
            # Create indexes for better performance
            self._create_indexes(cursor)
            
            # Create views for common queries
            self._create_views(cursor)
            
            # Commit all changes
            self.conn.commit()
            
            logger.info("Database schema created successfully")
            
            # Verify the creation
            self._verify_schema(cursor)
            
            return self.conn
            
        except Exception as e:
            logger.error(f"Failed to create database: {e}")
            if self.conn:
                self.conn.rollback()
            raise
    
    def _drop_existing_tables(self, cursor: sqlite3.Cursor):
        """
        Drop existing tables if they exist.
        
        We drop in reverse order of creation to respect foreign keys.
        
        Args:
            cursor: Database cursor
        """
        # List of tables in reverse dependency order
        tables = [
            'cart_items',
            'cart',
            'product_reviews',
            'inventory_log',
            'order_items',
            'orders',
            'products',
            'customers',
            'categories'
        ]
        
        # Also drop views
        views = [
            'customer_summary',
            'product_performance',
            'low_stock_products',
            'monthly_revenue'
        ]
        
        logger.info("Dropping existing tables and views...")
        
        # Drop views first
        for view in views:
            try:
                cursor.execute(f"DROP VIEW IF EXISTS {view}")
                logger.debug(f"Dropped view if exists: {view}")
            except sqlite3.Error as e:
                logger.warning(f"Could not drop view {view}: {e}")
        
        # Drop tables
        for table in tables:
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {table}")
                logger.debug(f"Dropped table if exists: {table}")
            except sqlite3.Error as e:
                logger.warning(f"Could not drop table {table}: {e}")
    
    def _create_categories_table(self, cursor: sqlite3.Cursor):
        """
        Create the categories table.
        
        This table organizes products into logical groups.
        
        Args:
            cursor: Database cursor
        """
        cursor.execute('''
        CREATE TABLE categories (
            category_id INTEGER PRIMARY KEY AUTOINCREMENT,
            category_name VARCHAR(100) NOT NULL UNIQUE,
            description TEXT,
            parent_category_id INTEGER,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (parent_category_id) REFERENCES categories(category_id)
        )
        ''')
        
        logger.info("Created categories table")
    
    def _create_customers_table(self, cursor: sqlite3.Cursor):
        """
        Create the customers table.
        
        This table stores all customer information.
        
        Args:
            cursor: Database cursor
        """
        cursor.execute('''
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name VARCHAR(50) NOT NULL,
            last_name VARCHAR(50) NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            phone VARCHAR(20),
            password_hash VARCHAR(255),  -- In production, store hashed passwords
            address TEXT,
            city VARCHAR(50),
            state VARCHAR(50),
            zip_code VARCHAR(10),
            country VARCHAR(50) DEFAULT 'USA',
            date_of_birth DATE,
            gender VARCHAR(10),
            is_active BOOLEAN DEFAULT TRUE,
            email_verified BOOLEAN DEFAULT FALSE,
            loyalty_points INTEGER DEFAULT 0,
            customer_type VARCHAR(20) DEFAULT 'regular',  -- regular, premium, vip
            preferred_payment_method VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_login_at TIMESTAMP,
            
            -- Indexes on commonly searched fields are created separately
            CHECK (email LIKE '%@%'),  -- Basic email validation
            CHECK (loyalty_points >= 0)  -- Points cannot be negative
        )
        ''')
        
        logger.info("Created customers table")
    
    def _create_products_table(self, cursor: sqlite3.Cursor):
        """
        Create the products table.
        
        This table stores all product information including inventory.
        
        Args:
            cursor: Database cursor
        """
        cursor.execute('''
        CREATE TABLE products (
            product_id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_name VARCHAR(200) NOT NULL,
            category_id INTEGER,
            sku VARCHAR(50) UNIQUE,  -- Stock Keeping Unit
            price DECIMAL(10, 2) NOT NULL,
            cost DECIMAL(10, 2),  -- Cost to business (for profit calculations)
            stock_quantity INTEGER DEFAULT 0,
            reserved_quantity INTEGER DEFAULT 0,  -- Items in active carts
            reorder_level INTEGER DEFAULT 10,  -- When to reorder
            description TEXT,
            brand VARCHAR(100),
            weight DECIMAL(10, 3),  -- In kg
            dimensions VARCHAR(50),  -- LxWxH format
            color VARCHAR(50),
            size VARCHAR(20),
            material VARCHAR(100),
            is_active BOOLEAN DEFAULT TRUE,
            is_featured BOOLEAN DEFAULT FALSE,
            discount_percentage DECIMAL(5, 2) DEFAULT 0,
            tax_rate DECIMAL(5, 2) DEFAULT 0,
            rating_average DECIMAL(3, 2) DEFAULT 0,  -- Average from reviews
            rating_count INTEGER DEFAULT 0,
            view_count INTEGER DEFAULT 0,  -- Product page views
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            launch_date DATE,
            discontinue_date DATE,
            
            FOREIGN KEY (category_id) REFERENCES categories(category_id),
            CHECK (price >= 0),
            CHECK (stock_quantity >= 0),
            CHECK (reserved_quantity >= 0),
            CHECK (discount_percentage >= 0 AND discount_percentage <= 100),
            CHECK (rating_average >= 0 AND rating_average <= 5)
        )
        ''')
        
        logger.info("Created products table")
    
    def _create_orders_table(self, cursor: sqlite3.Cursor):
        """
        Create the orders table.
        
        This table stores order header information.
        
        Args:
            cursor: Database cursor
        """
        cursor.execute('''
        CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER NOT NULL,
            order_number VARCHAR(50) UNIQUE,  -- Human-readable order number
            order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            required_date DATE,  -- When customer needs the order
            shipped_date TIMESTAMP,
            delivered_date TIMESTAMP,
            status VARCHAR(50) DEFAULT 'pending',  -- pending, confirmed, processing, shipped, delivered, cancelled, refunded
            payment_status VARCHAR(50) DEFAULT 'pending',  -- pending, paid, failed, refunded
            payment_method VARCHAR(50),  -- credit_card, debit_card, paypal, apple_pay, google_pay, bank_transfer
            payment_transaction_id VARCHAR(100),
            subtotal DECIMAL(10, 2),  -- Before tax and shipping
            tax_amount DECIMAL(10, 2),
            shipping_cost DECIMAL(10, 2),
            discount_amount DECIMAL(10, 2),
            total_amount DECIMAL(10, 2),  -- Final amount
            currency VARCHAR(3) DEFAULT 'USD',
            shipping_method VARCHAR(50),  -- standard, express, overnight
            shipping_address TEXT,
            shipping_city VARCHAR(50),
            shipping_state VARCHAR(50),
            shipping_zip VARCHAR(10),
            shipping_country VARCHAR(50),
            billing_address TEXT,
            billing_city VARCHAR(50),
            billing_state VARCHAR(50),
            billing_zip VARCHAR(10),
            billing_country VARCHAR(50),
            tracking_number VARCHAR(100),
            notes TEXT,  -- Customer notes
            internal_notes TEXT,  -- Staff notes
            ip_address VARCHAR(45),  -- For fraud detection
            user_agent TEXT,  -- Browser info for analytics
            referrer_source VARCHAR(100),  -- Where customer came from
            coupon_code VARCHAR(50),
            gift_message TEXT,
            is_gift BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
            CHECK (total_amount >= 0),
            CHECK (status IN ('pending', 'confirmed', 'processing', 'shipped', 'delivered', 'cancelled', 'refunded')),
            CHECK (payment_status IN ('pending', 'paid', 'failed', 'refunded'))
        )
        ''')
        
        logger.info("Created orders table")
    
    def _create_order_items_table(self, cursor: sqlite3.Cursor):
        """
        Create the order_items table.
        
        This table stores individual line items for each order.
        
        Args:
            cursor: Database cursor
        """
        cursor.execute('''
        CREATE TABLE order_items (
            order_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            unit_price DECIMAL(10, 2) NOT NULL,  -- Price at time of purchase
            discount_amount DECIMAL(10, 2) DEFAULT 0,
            tax_amount DECIMAL(10, 2) DEFAULT 0,
            subtotal DECIMAL(10, 2) NOT NULL,  -- quantity * unit_price - discount
            total DECIMAL(10, 2) NOT NULL,  -- subtotal + tax
            is_gift BOOLEAN DEFAULT FALSE,
            gift_wrap BOOLEAN DEFAULT FALSE,
            notes TEXT,
            fulfillment_status VARCHAR(50) DEFAULT 'pending',  -- pending, packed, shipped, delivered
            return_status VARCHAR(50),  -- requested, approved, received, refunded
            return_reason TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE,
            FOREIGN KEY (product_id) REFERENCES products(product_id),
            CHECK (quantity > 0),
            CHECK (unit_price >= 0),
            CHECK (subtotal >= 0)
        )
        ''')
        
        logger.info("Created order_items table")
    
    def _create_inventory_log_table(self, cursor: sqlite3.Cursor):
        """
        Create the inventory_log table.
        
        This table tracks all inventory movements for audit purposes.
        
        Args:
            cursor: Database cursor
        """
        cursor.execute('''
        CREATE TABLE inventory_log (
            log_id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            change_type VARCHAR(50) NOT NULL,  -- purchase, sale, return, adjustment, damage, theft
            quantity_change INTEGER NOT NULL,  -- Positive for additions, negative for removals
            quantity_before INTEGER NOT NULL,
            quantity_after INTEGER NOT NULL,
            reference_type VARCHAR(50),  -- order, return, adjustment, etc.
            reference_id INTEGER,  -- ID of the related record
            notes TEXT,
            performed_by VARCHAR(100),  -- User who made the change
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        )
        ''')
        
        logger.info("Created inventory_log table")
    
    def _create_product_reviews_table(self, cursor: sqlite3.Cursor):
        """
        Create the product_reviews table.
        
        This table stores customer reviews and ratings for products.
        
        Args:
            cursor: Database cursor
        """
        cursor.execute('''
        CREATE TABLE product_reviews (
            review_id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            customer_id INTEGER NOT NULL,
            order_id INTEGER,  -- Link to verified purchase
            rating INTEGER NOT NULL,
            title VARCHAR(200),
            comment TEXT,
            is_verified_purchase BOOLEAN DEFAULT FALSE,
            is_recommended BOOLEAN DEFAULT TRUE,
            helpful_count INTEGER DEFAULT 0,
            not_helpful_count INTEGER DEFAULT 0,
            is_featured BOOLEAN DEFAULT FALSE,
            status VARCHAR(50) DEFAULT 'pending',  -- pending, approved, rejected
            moderation_notes TEXT,
            response_from_seller TEXT,
            response_date TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            FOREIGN KEY (product_id) REFERENCES products(product_id),
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
            FOREIGN KEY (order_id) REFERENCES orders(order_id),
            CHECK (rating >= 1 AND rating <= 5),
            UNIQUE(product_id, customer_id, order_id)  -- One review per product per order
        )
        ''')
        
        logger.info("Created product_reviews table")
    
    def _create_cart_table(self, cursor: sqlite3.Cursor):
        """
        Create the cart and cart_items tables.
        
        These tables store shopping cart information for customers.
        
        Args:
            cursor: Database cursor
        """
        # Cart header table
        cursor.execute('''
        CREATE TABLE cart (
            cart_id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER,
            session_id VARCHAR(100),  -- For anonymous users
            status VARCHAR(20) DEFAULT 'active',  -- active, abandoned, converted
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at TIMESTAMP,  -- When to clean up abandoned carts
            
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
            CHECK (customer_id IS NOT NULL OR session_id IS NOT NULL)  -- Must have either customer or session
        )
        ''')
        
        # Cart items table
        cursor.execute('''
        CREATE TABLE cart_items (
            cart_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
            cart_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL DEFAULT 1,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            saved_for_later BOOLEAN DEFAULT FALSE,
            
            FOREIGN KEY (cart_id) REFERENCES cart(cart_id) ON DELETE CASCADE,
            FOREIGN KEY (product_id) REFERENCES products(product_id),
            CHECK (quantity > 0),
            UNIQUE(cart_id, product_id)  -- One entry per product per cart
        )
        ''')
        
        logger.info("Created cart and cart_items tables")
    
    def _create_indexes(self, cursor: sqlite3.Cursor):
        """
        Create indexes for better query performance.
        
        These indexes speed up common queries and joins.
        
        Args:
            cursor: Database cursor
        """
        indexes = [
            # Customer indexes
            "CREATE INDEX idx_customers_email ON customers(email)",
            "CREATE INDEX idx_customers_phone ON customers(phone)",
            "CREATE INDEX idx_customers_city_state ON customers(city, state)",
            "CREATE INDEX idx_customers_created_at ON customers(created_at)",
            
            # Product indexes
            "CREATE INDEX idx_products_category ON products(category_id)",
            "CREATE INDEX idx_products_brand ON products(brand)",
            "CREATE INDEX idx_products_sku ON products(sku)",
            "CREATE INDEX idx_products_price ON products(price)",
            "CREATE INDEX idx_products_stock ON products(stock_quantity)",
            "CREATE INDEX idx_products_active ON products(is_active)",
            "CREATE INDEX idx_products_featured ON products(is_featured)",
            "CREATE INDEX idx_products_rating ON products(rating_average DESC)",
            
            # Order indexes
            "CREATE INDEX idx_orders_customer ON orders(customer_id)",
            "CREATE INDEX idx_orders_date ON orders(order_date)",
            "CREATE INDEX idx_orders_status ON orders(status)",
            "CREATE INDEX idx_orders_payment_status ON orders(payment_status)",
            "CREATE INDEX idx_orders_number ON orders(order_number)",
            
            # Order items indexes
            "CREATE INDEX idx_order_items_order ON order_items(order_id)",
            "CREATE INDEX idx_order_items_product ON order_items(product_id)",
            
            # Review indexes
            "CREATE INDEX idx_reviews_product ON product_reviews(product_id)",
            "CREATE INDEX idx_reviews_customer ON product_reviews(customer_id)",
            "CREATE INDEX idx_reviews_rating ON product_reviews(rating)",
            "CREATE INDEX idx_reviews_status ON product_reviews(status)",
            
            # Cart indexes
            "CREATE INDEX idx_cart_customer ON cart(customer_id)",
            "CREATE INDEX idx_cart_session ON cart(session_id)",
            "CREATE INDEX idx_cart_status ON cart(status)",
            
            # Inventory log indexes
            "CREATE INDEX idx_inventory_log_product ON inventory_log(product_id)",
            "CREATE INDEX idx_inventory_log_date ON inventory_log(created_at)",
            "CREATE INDEX idx_inventory_log_type ON inventory_log(change_type)"
        ]
        
        logger.info("Creating indexes...")
        
        for index in indexes:
            try:
                cursor.execute(index)
                index_name = index.split()[2]  # Extract index name
                logger.debug(f"Created index: {index_name}")
            except sqlite3.Error as e:
                logger.warning(f"Could not create index: {e}")
    
    def _create_views(self, cursor: sqlite3.Cursor):
        """
        Create views for common queries.
        
        Views simplify complex queries and improve performance.
        
        Args:
            cursor: Database cursor
        """
        # Customer summary view
        cursor.execute('''
        CREATE VIEW customer_summary AS
        SELECT 
            c.customer_id,
            c.first_name || ' ' || c.last_name as full_name,
            c.email,
            c.customer_type,
            COUNT(DISTINCT o.order_id) as total_orders,
            COALESCE(SUM(o.total_amount), 0) as lifetime_value,
            COALESCE(AVG(o.total_amount), 0) as avg_order_value,
            MAX(o.order_date) as last_order_date,
            c.created_at as customer_since
        FROM customers c
        LEFT JOIN orders o ON c.customer_id = o.customer_id AND o.status != 'cancelled'
        GROUP BY c.customer_id
        ''')
        
        logger.info("Created customer_summary view")
        
        # Product performance view
        cursor.execute('''
        CREATE VIEW product_performance AS
        SELECT 
            p.product_id,
            p.product_name,
            p.brand,
            c.category_name,
            p.price,
            p.stock_quantity,
            COALESCE(SUM(oi.quantity), 0) as units_sold,
            COALESCE(SUM(oi.total), 0) as revenue,
            p.rating_average,
            p.rating_count,
            p.view_count
        FROM products p
        LEFT JOIN categories c ON p.category_id = c.category_id
        LEFT JOIN order_items oi ON p.product_id = oi.product_id
        LEFT JOIN orders o ON oi.order_id = o.order_id AND o.status != 'cancelled'
        GROUP BY p.product_id
        ''')
        
        logger.info("Created product_performance view")
        
        # Low stock products view
        cursor.execute('''
        CREATE VIEW low_stock_products AS
        SELECT 
            p.product_id,
            p.product_name,
            p.sku,
            p.stock_quantity,
            p.reserved_quantity,
            p.reorder_level,
            (p.stock_quantity - p.reserved_quantity) as available_quantity,
            c.category_name
        FROM products p
        LEFT JOIN categories c ON p.category_id = c.category_id
        WHERE p.is_active = TRUE 
        AND (p.stock_quantity - p.reserved_quantity) <= p.reorder_level
        ORDER BY (p.stock_quantity - p.reserved_quantity) ASC
        ''')
        
        logger.info("Created low_stock_products view")
        
        # Monthly revenue view
        cursor.execute('''
        CREATE VIEW monthly_revenue AS
        SELECT 
            strftime('%Y-%m', order_date) as month,
            COUNT(DISTINCT order_id) as total_orders,
            COUNT(DISTINCT customer_id) as unique_customers,
            SUM(total_amount) as revenue,
            AVG(total_amount) as avg_order_value
        FROM orders
        WHERE status NOT IN ('cancelled', 'refunded')
        GROUP BY strftime('%Y-%m', order_date)
        ORDER BY month DESC
        ''')
        
        logger.info("Created monthly_revenue view")
    
    def _verify_schema(self, cursor: sqlite3.Cursor):
        """
        Verify that all tables were created successfully.
        
        Args:
            cursor: Database cursor
        """
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' 
            ORDER BY name
        """)
        
        tables = cursor.fetchall()
        table_names = [table[0] for table in tables]
        
        expected_tables = [
            'cart', 'cart_items', 'categories', 'customers',
            'inventory_log', 'order_items', 'orders',
            'product_reviews', 'products'
        ]
        
        for expected_table in expected_tables:
            if expected_table not in table_names:
                logger.warning(f"Expected table '{expected_table}' not found in schema")
            else:
                # Get row count for each table
                cursor.execute(f"SELECT COUNT(*) FROM {expected_table}")
                count = cursor.fetchone()[0]
                logger.info(f"Table '{expected_table}' created successfully (rows: {count})")
    
    def get_schema_info(self) -> dict:
        """
        Get detailed information about the database schema.
        
        Returns:
            Dictionary containing schema information
        """
        if not self.conn:
            self.conn = sqlite3.connect(self.db_path)
        
        cursor = self.conn.cursor()
        schema_info = {}
        
        # Get all tables
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' 
            ORDER BY name
        """)
        
        tables = cursor.fetchall()
        
        for table_name in [t[0] for t in tables]:
            if table_name.startswith('sqlite_'):
                continue
                
            # Get column information
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            # Get foreign keys
            cursor.execute(f"PRAGMA foreign_key_list({table_name})")
            foreign_keys = cursor.fetchall()
            
            # Get indexes
            cursor.execute(f"PRAGMA index_list({table_name})")
            indexes = cursor.fetchall()
            
            schema_info[table_name] = {
                'columns': [
                    {
                        'name': col[1],
                        'type': col[2],
                        'nullable': not col[3],
                        'default': col[4],
                        'primary_key': bool(col[5])
                    }
                    for col in columns
                ],
                'foreign_keys': [
                    {
                        'column': fk[3],
                        'references_table': fk[2],
                        'references_column': fk[4]
                    }
                    for fk in foreign_keys
                ],
                'indexes': [idx[1] for idx in indexes]
            }
        
        return schema_info
    
    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


def main():
    """
    Main function to create the database when running this module directly.
    """
    # Create the database
    with DatabaseCreator() as creator:
        conn = creator.create_database()
        schema_info = creator.get_schema_info()
        
        print("\n" + "="*50)
        print("DATABASE CREATED SUCCESSFULLY")
        print("="*50)
        print(f"\nDatabase location: {creator.db_path}")
        print(f"Tables created: {len(schema_info)}")
        
        for table_name, info in schema_info.items():
            print(f"\n{table_name}:")
            print(f"  - Columns: {len(info['columns'])}")
            print(f"  - Foreign Keys: {len(info['foreign_keys'])}")
            print(f"  - Indexes: {len(info['indexes'])}")


if __name__ == "__main__":
    main()