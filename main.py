import redis
import csv
import logging
from typing import List, Dict, Tuple, Optional

class RedisClient:
    def __init__(self, host: str, port: int, username: str, password: str):
        """
        Initialize the Redis client with connection parameters.
        
        :param host: Redis server host
        :param port: Redis server port
        :param username: Redis username
        :param password: Redis password
        """
        self.redis = None
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        
        # Configure logging
        logging.basicConfig(level=logging.INFO, 
                            format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

    def connect(self) -> bool:
        """
        Establish a connection to the Redis database.
        
        :return: True if connection successful, False otherwise
        """
        try:
            self.redis = redis.StrictRedis(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                decode_responses=True
            )
            # Perform a simple ping to verify connection
            self.redis.ping()
            self.logger.info("Successfully connected to Redis.")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to Redis: {e}")
            return False

    def load_users(self, file_path: str) -> bool:
        """
        Load user data from a file into Redis.
        
        :param file_path: Path to the users file
        :return: True if loading successful, False otherwise
        """
        try:
            with open(file_path, 'r') as file:
                for line in file:
                    # Split the line, handling potential variations in delimiter
                    fields = [f.strip('"') for f in line.strip().split('" "')]
                    
                    # Ensure we have enough fields
                    if len(fields) < 3:
                        self.logger.warning(f"Skipping invalid line: {line}")
                        continue
                    
                    user_id = fields[0]
                    # Create a dictionary of user attributes
                    user_data = {fields[i]: fields[i+1] for i in range(1, len(fields)-1, 2)}
                    
                    # Store user data in Redis hash
                    self.redis.hset(user_id, mapping=user_data)
            
            self.logger.info("Successfully loaded users into Redis.")
            return True
        except Exception as e:
            self.logger.error(f"Error loading users: {e}")
            return False

    def load_scores(self, file_path: str) -> bool:
        """
        Load user scores from a CSV file into Redis sorted sets.
        
        :param file_path: Path to the user scores CSV file
        :return: True if loading successful, False otherwise
        """
        try:
            with open(file_path, 'r') as file:
                reader = csv.reader(file)
                next(reader)  # Skip header row
                
                for row in reader:
                    # Validate row has required fields
                    if len(row) < 3:
                        self.logger.warning(f"Skipping invalid row: {row}")
                        continue
                    
                    leaderboard, user_id, score = row[0], row[1], int(row[2])
                    self.redis.zadd(leaderboard, {user_id: score})
            
            self.logger.info("Successfully loaded scores into Redis.")
            return True
        except Exception as e:
            self.logger.error(f"Error loading scores: {e}")
            return False

    def get_user_data(self, user_id: str) -> Optional[Dict[str, str]]:
        """
        Retrieve all attributes for a specific user.
        
        :param user_id: User identifier
        :return: Dictionary of user attributes or None
        """
        try:
            user_data = self.redis.hgetall(user_id)
            return user_data if user_data else None
        except Exception as e:
            self.logger.error(f"Error retrieving user data: {e}")
            return None

    def get_user_coordinates(self, user_id: str) -> Optional[Tuple[str, str]]:
        """
        Get the coordinates (longitude and latitude) for a user.
        
        :param user_id: User identifier
        :return: Tuple of (longitude, latitude) or None
        """
        try:
            longitude = self.redis.hget(user_id, 'longitude')
            latitude = self.redis.hget(user_id, 'latitude')
            return (longitude, latitude) if longitude and latitude else None
        except Exception as e:
            self.logger.error(f"Error retrieving user coordinates: {e}")
            return None

    def get_users_by_even_id(self) -> Tuple[List[str], List[str]]:
        """
        Retrieve users with even IDs, returning their keys and last names.
        
        :return: Tuple of (keys, last_names)
        """
        try:
            keys, last_names = [], []
            for key in self.redis.scan_iter("user:*"):
                if int(key.split(':')[1]) % 2 == 0:
                    keys.append(key)
                    last_names.append(self.redis.hget(key, 'last_name'))
            return keys, last_names
        except Exception as e:
            self.logger.error(f"Error retrieving users by even ID: {e}")
            return [], []

    def get_female_users_in_region(self, countries: List[str], min_lat: float, max_lat: float) -> List[Dict[str, str]]:
        """
        Find female users in specified countries within a latitude range.
        
        :param countries: List of countries to search
        :param min_lat: Minimum latitude
        :param max_lat: Maximum latitude
        :return: List of matching user records
        """
        try:
            matching_users = []
            for key in self.redis.scan_iter("user:*"):
                user_data = self.redis.hgetall(key)
                # Validate all required fields exist
                if (user_data.get('gender') == 'female' and 
                    user_data.get('country') in countries and 
                    min_lat <= float(user_data.get('latitude', 0)) <= max_lat):
                    matching_users.append(user_data)
            return matching_users
        except Exception as e:
            self.logger.error(f"Error retrieving female users in region: {e}")
            return []

    def get_top_players(self, leaderboard: str, limit: int = 10) -> List[str]:
        """
        Retrieve email IDs of top players from a specific leaderboard.
        
        :param leaderboard: Leaderboard identifier
        :param limit: Number of top players to retrieve
        :return: List of email IDs
        """
        try:
            top_players = self.redis.zrevrange(leaderboard, 0, limit-1, withscores=False)
            return [self.redis.hget(player, 'email') for player in top_players]
        except Exception as e:
            self.logger.error(f"Error retrieving top players: {e}")
            return []

    def clear_database(self) -> bool:
        """
        Delete all data from the Redis database.
        
        :return: True if successful, False otherwise
        """
        try:
            self.redis.flushdb()
            self.logger.info("Database has been cleared.")
            return True
        except Exception as e:
            self.logger.error(f"Error clearing database: {e}")
            return False

# Example usage
def main():
    # Connection details should be stored securely, not hardcoded
    redis_client = RedisClient(
        host='redis-16929.c124.us-central1-1.gce.redns.redis-cloud.com',
        port=19661,
        username='default',
        password='CNPO4piptOIaGMkRW6AjCEILhtozQAUa'
    )
    
    # Connect to Redis
    if redis_client.connect():
        # Load data
        redis_client.load_users('users.txt')
        redis_client.load_scores('userscores.csv')
        
        # Example queries
        print(redis_client.get_user_data('user:1'))
        print(redis_client.get_top_players('leaderboard:2'))

if __name__ == '__main__':
    main()