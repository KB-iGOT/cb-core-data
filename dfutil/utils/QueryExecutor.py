import sys
from pathlib import Path
import duckdb
import os
from datetime import datetime
import pandas as pd
# Add root directory to sys.path for absolute imports
sys.path.append(str(Path(__file__).resolve().parents[2]))


from constants.QueryConstants import QueryConstants

class QueryExecutor:
    def __init__(self, output_dir="query_results"):
        """Initialize the query executor with DuckDB connection"""
        self.conn = duckdb.connect()
        self.output_dir = output_dir
        self.results = {}
        
        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
    
    def execute_query(self, query_name, query):
        """Execute a single query and return the result"""
        try:
            print(f"\n🔄 Executing: {query_name}")
            print("-" * 50)
            # Execute query
            result = self.conn.execute(query).fetchdf()
            
            print(f"✅ Success: {len(result)} rows returned")
            print(f"📊 Preview:")
            print(result.head() if len(result) > 0 else "No data returned")
            
            # Store result
            self.results[query_name] = result
            
            return result
            
        except Exception as e:
            print(f"❌ Error executing {query_name}: {str(e)}")
            self.results[query_name] = f"ERROR: {str(e)}"
            return None
    
    def execute_query_list(self, query_list, category_name):
        """Execute all queries in a list"""
        print(f"\n{'='*60}")
        print(f"🚀 EXECUTING {category_name.upper()} QUERIES")
        print(f"{'='*60}")
        print(len(query_list))
        for query_name, query in enumerate(query_list):
            self.execute_query(query_name, query)
    
    def execute_all_queries(self):
        """Execute all queries in sequence"""
        print("🎯 Starting Query Execution Pipeline")
        print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Execute each category
        self.execute_query_list(QueryConstants.ORG_BASED_LIST, "ORG BASED")
        self.execute_query_list(QueryConstants.USER_BASED_LIST, "USER BASED") 
        self.execute_query_list(QueryConstants.COURSE_BASED_LIST, "COURSE BASED")
        self.execute_query_list(QueryConstants.ENROLMENT_BASED_LIST, "ENROLMENT BASED")
        
        print(f"\n🏁 Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    def save_results_to_csv(self):
        """Save all results to CSV files"""
        print(f"\n💾 Saving results to {self.output_dir}/")
        
        for query_name, result in self.results.items():
            if isinstance(result, pd.DataFrame):
                filename = f"{self.output_dir}/{query_name}.csv"
                result.to_csv(filename, index=False)
                print(f"📁 Saved: {filename}")
    
    def save_results_to_json(self):
        """Save all results to JSON files"""
        print(f"\n💾 Saving results to JSON in {self.output_dir}/")
        
        for query_name, result in self.results.items():
            if isinstance(result, pd.DataFrame):
                filename = f"{self.output_dir}/{query_name}.json"
                result.to_json(filename, orient='records', indent=2)
                print(f"📁 Saved: {filename}")
    
    def print_summary(self):
        """Print execution summary"""
        print(f"\n{'='*60}")
        print("📋 EXECUTION SUMMARY")
        print(f"{'='*60}")
        
        successful = 0
        failed = 0
        
        for query_name, result in self.results.items():
            if isinstance(result, pd.DataFrame):
                print(f"✅ {query_name}: {len(result)} rows")
                successful += 1
            else:
                print(f"❌ {query_name}: {result}")
                failed += 1
        
        print(f"\n📊 Total: {len(self.results)} queries")
        print(f"✅ Successful: {successful}")
        print(f"❌ Failed: {failed}")
    
    def close(self):
        """Close the database connection"""
        self.conn.close()

def main():
    """Main execution function"""
    print("🎯 Query Executor Starting...")
    
    # Initialize executor
    executor = QueryExecutor()
    
    try:
        # Execute all queries
        executor.execute_all_queries()
        
        # Save results
        executor.save_results_to_csv()
        executor.save_results_to_json()
        
        # Print summary
        executor.print_summary()
        
    except KeyboardInterrupt:
        print("\n⚠️ Execution interrupted by user")
    except Exception as e:
        print(f"\n💥 Unexpected error: {str(e)}")
    finally:
        # Clean up
        executor.close()
        print("\n👋 Query execution completed!")

if __name__ == "__main__":
    main()