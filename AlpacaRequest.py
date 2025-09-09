from datetime import datetime
import requests
import asyncio
from typing import Callable, List, Dict, Any, Optional


class AlpacaRequest:
    def __init__(
        self, 
        api_key: str, 
        secret_key: str, 
        before_response_callback: Optional[Callable] = None,
        after_response_callback: Optional[Callable] = None,
        before_record_callback: Optional[Callable] = None,
        after_record_callback: Optional[Callable] = None
    ):
        """
        Initialize the AlpacaRequest class with before and after callbacks.
        
        Args:
            api_key: Alpaca API key
            secret_key: Alpaca secret key
            before_response_callback: Called before processing a batch of records
            after_response_callback: Called after processing a batch of records
            before_record_callback: Called before processing each individual record
            after_record_callback: Called after processing each individual record
        """
        self.api_key = api_key
        self.secret_key = secret_key
        self.before_response_callback = before_response_callback
        self.after_response_callback = after_response_callback
        self.before_record_callback = before_record_callback
        self.after_record_callback = after_record_callback
        self.loop = asyncio.get_event_loop()

    async def get_all_async(
        self, 
        symbol: str, 
        start_date: datetime, 
        end_date: datetime, 
        transaction_type: str = 'trades', 
        limit: int = 10000,
        bar_timeframe: str = None
    ) -> List[Dict[str, Any]]:
        """
        Asynchronously get all data for a symbol between start_date and end_date.
        
        Args:
            symbol: The stock symbol
            start_date: Start date
            end_date: End date
            transaction_type: Type of transaction ('trades' or 'quotes')
            limit: Number of records to fetch per page
            
        Returns:
            List of all records
        """
        # Initialize an empty list to store all records
        all_records = []

        # Convert datetime to str 'YYYY-MM-DDT00:00:00Z'
        start_date_str = start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        end_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        async def fetch_page(next_page_token=None):
            print(f"Fetching {transaction_type} for {symbol} between {start_date_str} and {end_date_str} page: {next_page_token}")

            # Create URL with parameters
            url = f"https://data.alpaca.markets/v2/stocks/{transaction_type}?symbols={symbol}&start={start_date_str}&end={end_date_str}&limit={limit}&feed=sip&sort=asc"
            
            if transaction_type == 'bars' and bar_timeframe is None:
                raise ValueError("bar_timeframe must be provided for transaction_type 'bars'")
            
            if bar_timeframe and transaction_type == 'bars':
                url += f"&timeframe={bar_timeframe}"

            if next_page_token:
                url += f"&page_token={next_page_token}"

            headers = {
                "accept": "application/json",
                "APCA-API-KEY-ID": self.api_key,
                "APCA-API-SECRET-KEY": self.secret_key
            }

            # --- Response Callbacks ---
            # Before response callback
            request_id = None
            if self.before_response_callback:
                if asyncio.iscoroutinefunction(self.before_response_callback):
                    request_id = await self.before_response_callback(url)
                else:
                    request_id = await asyncio.to_thread(self.before_response_callback, url)            

            # Run the HTTP request in a thread pool to avoid blocking
            response = await asyncio.to_thread(
                lambda: requests.get(url, headers=headers)
            )

            if response.status_code != 200:
                print(f"HTTP Status: {response.status_code}")
                print(f"HTTP Headers: {dict(response.headers)}")                
                print(f"Error response: {response.text}")
                print(f"URL: {url}")
                return  # Skip processing this page on error
                
            result = response.json()
            
            # Process the records if they exist
            if transaction_type in result:
                response_records = result[transaction_type][symbol]
                
                # Store all records
                all_records.extend(response_records)
                
                
                # --- Record Callbacks ---
                # Process all records with before/after callbacks
                for record in response_records:
                    # Before record callback
                    if self.before_record_callback:
                        if asyncio.iscoroutinefunction(self.before_record_callback):
                            await self.before_record_callback(request_id, record)
                        else:
                            await asyncio.to_thread(self.before_record_callback, (request_id, record))
                    
                    # Main record processing could be added here
                    
                    # After record callback
                    if self.after_record_callback:
                        if asyncio.iscoroutinefunction(self.after_record_callback):
                            await self.after_record_callback(request_id, record)
                        else:
                            await asyncio.to_thread(self.after_record_callback, (request_id, record))
                
                # After response callback
                if self.after_response_callback:
                    if asyncio.iscoroutinefunction(self.after_response_callback):
                        await self.after_response_callback(request_id, response_records)
                    else:
                        await asyncio.to_thread(self.after_response_callback, (request_id, response_records))
            
            # If there's a next page, recursively fetch it
            if "next_page_token" in result and result["next_page_token"] is not None:
                await fetch_page(result["next_page_token"])
        
        # Start the recursive fetching with no page token
        await fetch_page()
        
        # Return all collected records
        return all_records

    def get_all(
        self, 
        symbol: str, 
        start_date: datetime, 
        end_date: datetime, 
        transaction_type: str = 'trades', 
        limit: int = 10000
    ) -> List[Dict[str, Any]]:
        """
        Synchronous wrapper for get_all_async.
        """
        return asyncio.run(self.get_all_async(
            symbol, start_date, end_date, transaction_type, limit
        ))

    def get_all_quotes(
        self, 
        symbol: str, 
        start_date: datetime, 
        end_date: datetime, 
        limit: int = 10000
    ) -> List[Dict[str, Any]]:
        """
        Get all quotes for a symbol.
        """
        return self.get_all(symbol, start_date, end_date, 'quotes', limit)
    
    def get_all_trades(
        self, 
        symbol: str, 
        start_date: datetime, 
        end_date: datetime, 
        limit: int = 10000
    ) -> List[Dict[str, Any]]:
        """
        Get all trades for a symbol.
        """
        return self.get_all(symbol, start_date, end_date, 'trades', limit)
