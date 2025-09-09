import asyncio
import uuid
from datetime import datetime
import json
import os
from typing import Optional, Dict, Any, Union, List

import asyncpg
from asyncpg import Pool, Connection

import dotenv
dotenv.load_dotenv()

class HttpRequestLog:
    """
    A class to log HTTP requests and responses to a PostgreSQL database.
    Also supports logging related data points associated with requests.
    """
    
    def __init__(self, db_config: Dict[str, str]):
        """
        Initialize the HttpRequestLog class.
        
        Args:
            db_config: Dictionary containing database connection parameters:
                       host, port, user, password, database
        """
        self.db_config = db_config
        self.pool: Optional[Pool] = None
    
    async def initialize(self) -> None:
        """
        Initialize the database connection pool and create the necessary tables if they don't exist.
        """
        # Create connection pool with explicit configuration
        self.pool = await asyncpg.create_pool(
            **self.db_config,
            min_size=5,  # Minimum number of connections
            max_size=20, # Maximum number of connections
            command_timeout=60  # Allow longer for schema operations
        )
        
        # Create the tables if they don't exist
        async with self.pool.acquire() as conn:
            # Print connection info for debugging
            db_name = await conn.fetchval("SELECT current_database()")
            schema = await conn.fetchval("SELECT current_schema()")
            print(f"Connected to database: {db_name}, schema: {schema}")
            
            # Use explicit transaction for schema changes
            async with conn.transaction():
                # 1. Create http_request_log table
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS http_request_log (
                        id UUID PRIMARY KEY,
                        request_time TIMESTAMP WITH TIME ZONE NOT NULL,
                        response_time TIMESTAMP WITH TIME ZONE,
                        url TEXT NOT NULL,
                        http_response JSONB
                    )
                ''')
                
                # Create an index on request_time for faster queries
                await conn.execute('''
                    CREATE INDEX IF NOT EXISTS idx_http_request_log_request_time 
                    ON http_request_log (request_time)
                ''')
                
                # 2. Create data_point table
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS data_point (
                        id UUID PRIMARY KEY,
                        id_request UUID NOT NULL,
                        data_point_type TEXT NOT NULL,
                        data_point JSONB NOT NULL,
                        created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (id_request) REFERENCES http_request_log (id) ON DELETE CASCADE
                    )
                ''')
                
                # Create indexes for data_point table
                await conn.execute('''
                    CREATE INDEX IF NOT EXISTS idx_data_point_id_request 
                    ON data_point (id_request)
                ''')
                
                await conn.execute('''
                    CREATE INDEX IF NOT EXISTS idx_data_point_type 
                    ON data_point (data_point_type)
                ''')
                
                # Verify tables exist
                tables_exist = await conn.fetch(
                    "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename IN ('http_request_log', 'data_point')"
                )
                table_names = [row['tablename'] for row in tables_exist]
                print(f"Tables created: {table_names}")
    
    async def close(self) -> None:
        """
        Close the database connection pool.
        """
        if self.pool:
            await self.pool.close()
            print("Database connection pool closed")
    
    async def log_request_start(self, log_id: uuid.UUID, url: str) -> None:
        """
        Log the start of an HTTP request.
        
        Args:
            log_id: The ID of the request
            url: The URL being requested
        """
        if not self.pool:
            raise RuntimeError("Database connection not initialized. Call initialize() first.")
        
        request_time = datetime.now()
        
        try:
            async with self.pool.acquire() as conn:
                # Use explicit transaction
                async with conn.transaction():
                    await conn.execute('''
                        INSERT INTO http_request_log (id, request_time, url)
                        VALUES ($1, $2, $3)
                    ''', log_id, request_time, url)
                    
                    # Verify insertion
                    count = await conn.fetchval(
                        'SELECT COUNT(*) FROM http_request_log WHERE id = $1', 
                        log_id
                    )
                    
                    if count == 1:
                        print(f"Request log entry created: {log_id}")
                    else:
                        print(f"WARNING: Failed to create request log entry: {log_id}")
        except Exception as e:
            print(f"Error logging request start: {e}")
            raise
    
    async def log_request_complete(self, log_id: uuid.UUID, http_response: Dict[str, Any]) -> None:
        """
        Log the completion of an HTTP request.
        
        Args:
            log_id: The UUID of the log entry to update
            http_response: Dictionary containing the HTTP response data
        """
        if not self.pool:
            raise RuntimeError("Database connection not initialized. Call initialize() first.")
        
        response_time = datetime.now()
        
        try:
            async with self.pool.acquire() as conn:
                # Use explicit transaction
                async with conn.transaction():
                    # Convert response to JSON string for storage
                    response_json = json.dumps(http_response)
                    
                    await conn.execute('''
                        UPDATE http_request_log
                        SET response_time = $1, http_response = $2
                        WHERE id = $3
                    ''', response_time, response_json, log_id)
                    
                    # Verify update
                    count = await conn.fetchval(
                        'SELECT COUNT(*) FROM http_request_log WHERE id = $1 AND response_time IS NOT NULL', 
                        log_id
                    )
                    
                    if count == 1:
                        print(f"Request log entry updated: {log_id}")
                    else:
                        print(f"WARNING: Failed to update request log entry: {log_id}")
        except Exception as e:
            print(f"Error logging request completion: {e}")
            raise
    
    async def log_data_point(self, request_id: uuid.UUID, data_point_type: str, data_point: Dict[str, Any]) -> uuid.UUID:
        """
        Log a data point associated with a request.
        
        Args:
            request_id: The UUID of the associated request
            data_point_type: Type of data point (e.g., 'trade', 'quote', 'analysis')
            data_point: Dictionary containing the data point information
            
        Returns:
            UUID of the created data point
        """
        if not self.pool:
            raise RuntimeError("Database connection not initialized. Call initialize() first.")
        
        data_point_id = uuid.uuid4()
        created_at = datetime.now()
        
        try:
            async with self.pool.acquire() as conn:
                # First verify the request exists
                async with conn.transaction():
                    request_exists = await conn.fetchval(
                        'SELECT COUNT(*) FROM http_request_log WHERE id = $1', 
                        request_id
                    )
                    
                    if not request_exists:
                        raise ValueError(f"Request with ID {request_id} does not exist")
                    
                    # Convert data point to JSON
                    data_point_json = json.dumps(data_point)
                    
                    # Insert the data point
                    await conn.execute('''
                        INSERT INTO data_point (id, id_request, data_point_type, data_point, created_at)
                        VALUES ($1, $2, $3, $4, $5)
                    ''', data_point_id, request_id, data_point_type, data_point_json, created_at)
                    
                    # Verify insertion
                    count = await conn.fetchval(
                        'SELECT COUNT(*) FROM data_point WHERE id = $1', 
                        data_point_id
                    )
                    
                    if count == 1:
                        print(f"Data point logged: {data_point_id} for request: {request_id}")
                    else:
                        print(f"WARNING: Failed to log data point for request: {request_id}")
            
            return data_point_id
        except Exception as e:
            print(f"Error logging data point: {e}")
            raise
    
    async def get_data_points(self, request_id: uuid.UUID, data_point_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Retrieve data points for a specific request, optionally filtered by type.
        
        Args:
            request_id: The UUID of the request
            data_point_type: Optional type filter
            
        Returns:
            List of data point entries
        """
        if not self.pool:
            raise RuntimeError("Database connection not initialized. Call initialize() first.")
        
        try:
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    if data_point_type:
                        rows = await conn.fetch('''
                            SELECT * FROM data_point 
                            WHERE id_request = $1 AND data_point_type = $2
                            ORDER BY created_at
                        ''', request_id, data_point_type)
                    else:
                        rows = await conn.fetch('''
                            SELECT * FROM data_point 
                            WHERE id_request = $1
                            ORDER BY created_at
                        ''', request_id)
                    
                    result = []
                    for row in rows:
                        item = dict(row)
                        # Parse JSON data point if it exists
                        if item.get('data_point'):
                            try:
                                if isinstance(item['data_point'], str):
                                    item['data_point'] = json.loads(item['data_point'])
                            except json.JSONDecodeError:
                                pass
                        result.append(item)
                    
                    return result
        except Exception as e:
            print(f"Error retrieving data points: {e}")
            return []
    
    async def get_request_with_data_points(self, request_id: uuid.UUID) -> Optional[Dict[str, Any]]:
        """
        Retrieve a request log entry with all its associated data points.
        
        Args:
            request_id: The UUID of the request
            
        Returns:
            Dictionary containing the request data and associated data points
        """
        if not self.pool:
            raise RuntimeError("Database connection not initialized. Call initialize() first.")
        
        try:
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    # Get the request
                    request_row = await conn.fetchrow('''
                        SELECT * FROM http_request_log WHERE id = $1
                    ''', request_id)
                    
                    if not request_row:
                        return None
                    
                    request_data = dict(request_row)
                    
                    # Parse JSON response if it exists
                    if request_data.get('http_response'):
                        try:
                            if isinstance(request_data['http_response'], str):
                                request_data['http_response'] = json.loads(request_data['http_response'])
                        except json.JSONDecodeError:
                            pass
                    
                    # Get associated data points
                    data_point_rows = await conn.fetch('''
                        SELECT * FROM data_point 
                        WHERE id_request = $1
                        ORDER BY created_at
                    ''', request_id)
                    
                    data_points = []
                    for row in data_point_rows:
                        item = dict(row)
                        # Parse JSON data point
                        if item.get('data_point'):
                            try:
                                if isinstance(item['data_point'], str):
                                    item['data_point'] = json.loads(item['data_point'])
                            except json.JSONDecodeError:
                                pass
                        data_points.append(item)
                    
                    # Add data points to result
                    request_data['data_points'] = data_points
                    
                    return request_data
        except Exception as e:
            print(f"Error retrieving request with data points: {e}")
            return None
    
    async def get_log_entry(self, log_id: uuid.UUID) -> Optional[Dict[str, Any]]:
        """
        Retrieve a log entry by its ID.
        
        Args:
            log_id: The UUID of the log entry to retrieve
            
        Returns:
            Dictionary containing the log entry data, or None if not found
        """
        if not self.pool:
            raise RuntimeError("Database connection not initialized. Call initialize() first.")
        
        try:
            async with self.pool.acquire() as conn:
                # For read operations, transaction is optional but used for consistency
                async with conn.transaction():
                    row = await conn.fetchrow('''
                        SELECT * FROM http_request_log WHERE id = $1
                    ''', log_id)
                    
                    if row:
                        result = dict(row)
                        # Parse JSON response if it exists
                        if result.get('http_response'):
                            try:
                                # It might already be parsed by asyncpg
                                if isinstance(result['http_response'], str):
                                    result['http_response'] = json.loads(result['http_response'])
                            except json.JSONDecodeError:
                                pass
                        return result
                    return None
        except Exception as e:
            print(f"Error retrieving log entry: {e}")
            return None
    
    async def get_recent_logs(self, limit: int = 100) -> list:
        """
        Retrieve recent log entries.
        
        Args:
            limit: Maximum number of entries to retrieve
            
        Returns:
            List of log entries
        """
        if not self.pool:
            raise RuntimeError("Database connection not initialized. Call initialize() first.")
        
        try:
            async with self.pool.acquire() as conn:
                # For read operations, transaction is optional but used for consistency
                async with conn.transaction():
                    rows = await conn.fetch('''
                        SELECT * FROM http_request_log
                        ORDER BY request_time DESC
                        LIMIT $1
                    ''', limit)
                    
                    result = []
                    for row in rows:
                        item = dict(row)
                        # Parse JSON response if it exists
                        if item.get('http_response'):
                            try:
                                # It might already be parsed by asyncpg
                                if isinstance(item['http_response'], str):
                                    item['http_response'] = json.loads(item['http_response'])
                            except json.JSONDecodeError:
                                pass
                        result.append(item)
                    return result
        except Exception as e:
            print(f"Error retrieving recent logs: {e}")
            return []


# Example usage
async def example():
    # Database configuration
    db_config = {
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'port': os.getenv('POSTGRES_PORT', '5432'),
        'user': os.getenv('POSTGRES_USER', 'postgres'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'database': os.getenv('POSTGRES_DB', 'postgres')
    }
    
    # Initialize logger
    http_logger = HttpRequestLog(db_config)
    await http_logger.initialize()
    
    try:
        # Generate a UUID for the log entry
        request_id = uuid.uuid4()
        print(f"Created request ID: {request_id}")
        
        # Log a request start
        await http_logger.log_request_start(request_id, 'https://api.example.com/stocks/data')
        
        # Simulate API call
        await asyncio.sleep(0.5)
        
        # Log some data points associated with the request
        trade_data = {
            'symbol': 'AAPL',
            'price': 185.50,
            'volume': 1000,
            'timestamp': datetime.now().isoformat()
        }
        data_point_id = await http_logger.log_data_point(request_id, 'trade', trade_data)
        print(f"Created data point: {data_point_id}")
        
        quote_data = {
            'symbol': 'AAPL',
            'bid': 185.45,
            'ask': 185.55,
            'timestamp': datetime.now().isoformat()
        }
        await http_logger.log_data_point(request_id, 'quote', quote_data)
        
        # Log request completion
        response_data = {
            'status': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': {'message': 'Success', 'data': [trade_data, quote_data]}
        }
        await http_logger.log_request_complete(request_id, response_data)
        
        # Retrieve the complete request with its data points
        complete_request = await http_logger.get_request_with_data_points(request_id)
        print(f"Retrieved request with {len(complete_request.get('data_points', []))} data points")
        
        # Get only trade data points
        trade_data_points = await http_logger.get_data_points(request_id, 'trade')
        print(f"Retrieved {len(trade_data_points)} trade data points")
        
    finally:
        # Close connections
        await http_logger.close()

if __name__ == "__main__":
    asyncio.run(example())