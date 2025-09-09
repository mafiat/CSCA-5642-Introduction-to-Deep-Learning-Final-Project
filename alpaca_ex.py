import os
import json
import uuid
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

from AlpacaRequest import AlpacaRequest
from HttpRequestLog import HttpRequestLog


# Example usage
def main(symbol):


    api_key = os.getenv("ALPACA_API_ID_KEY")
    api_secret = os.getenv("ALPACA_API_SECRET_KEY")
    alpaca = AlpacaRequest(api_key, api_secret)

    start_date = "2025-03-03T09:30:00Z"
    end_date = "2025-03-03T09:35:00Z"

    datetime_run = datetime.now()

    # Create start_date and end_date into datetime object
    start_date = datetime.fromisoformat(start_date)
    end_date = datetime.fromisoformat(end_date)
    
    trades = alpaca.get_all(symbol, start_date, end_date)
    print(f"Retrieved {len(trades)} trades for {symbol}")

    # Write trades to a json file with the datetime in the file name
    with open(f"trades_{datetime_run.strftime('%Y-%m-%d_%H-%M-%S')}.json", "w") as f:
        f.write(json.dumps(trades, indent=4))
    
    quotes = alpaca.get_all(symbol, start_date, end_date, transaction_type='quotes')
    print(f"Retrieved {len(quotes)} quotes for {symbol}")

    # Write trades to a json file with the datetime in the file name
    with open(f"quotes_{datetime_run.strftime('%Y-%m-%d_%H-%M-%S')}.json", "w") as f:
        f.write(json.dumps(quotes, indent=4))

    # Process the trades as needed
    # ...

# Example usage
async def main_async(symbol):
    
    try:

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

        # Define example callbacks
        async def before_response_handler(url):
            print(f"Starting to process url: {url}")       
            # Log a request start
            request_id = uuid.uuid4()
            await http_logger.log_request_start(request_id, url) 
            print(f"Request ID: {request_id}")
            return request_id

            
        async def after_response_handler(request_id, response):
            print(f"Finished processing response of {len(response)} records")
            # Log request completion
            await http_logger.log_request_complete(request_id, response)
            print(f"Logged request ID: {request_id}")
        
        async def before_record_handler(request_id, record):
            print(f"Starting to process record ID: {record.get('i', 'unknown')}")
            
        async def after_record_handler(id_request, record):
            # Simulate database write with a delay
            await asyncio.sleep(0.01)  # Simulating some I/O operation
            print(f"Finished processing record ID: {record.get('i', 'unknown')}")
            await http_logger.log_data_point(id_request, 'trade', record)
            print(f"Logged record ID: {record.get('i', 'unknown')}")

        # Initialize the client
        client = AlpacaRequest(
            api_key=os.getenv("ALPACA_API_ID_KEY"),
            secret_key=os.getenv("ALPACA_API_SECRET_KEY"),
            before_response_callback=before_response_handler,
            after_response_callback=after_response_handler,
#            before_record_callback=before_record_handler,
#            after_record_callback=after_record_handler
        )
        
        # Example dates
        start = datetime(2023, 1, 1, 0, 0)
        end = datetime(2025, 3, 22, 0, 0)
        
        # Get trades asynchronously
#        trades = await client.get_all_async(symbol, start, end)
#        print(f"Retrieved {len(trades)} total trades")

        # Get bars asynchronously
        bars = await client.get_all_async(symbol, start, end, transaction_type='bars', bar_timeframe='5Min')
        print(f"Retrieved {len(bars)} total bars")

#        quotes = await client.get_all_async(symbol, start, end, transaction_type='quotes')
#        print(f"Retrieved {len(quotes)} total quotes")

    finally:
        # Close connections
        await http_logger.close()

if __name__ == "__main__":
    symbol = "SPY"
    # Check command line flag to run async
    import sys
    run_async = len(sys.argv) > 1 and sys.argv[1] == "--async"
    if run_async:
        import asyncio
        asyncio.run(main_async(symbol))
    else:
        main(symbol)
    