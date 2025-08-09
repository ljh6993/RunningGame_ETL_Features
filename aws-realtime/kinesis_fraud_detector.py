"""
Real-time Fraud Detection Pipeline using AWS Kinesis
Monitors game location events to detect suspicious behavior and location spoofing
"""

import json
import boto3
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict, deque
import math
import redis
import os
from concurrent.futures import ThreadPoolExecutor
import threading

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class LocationEvent:
    user_id: str
    timestamp: int
    latitude: float
    longitude: float
    accuracy: float
    speed: Optional[float]
    platform: str
    session_id: str
    device_id: str

@dataclass
class FraudAlert:
    user_id: str
    alert_type: str
    risk_score: float
    timestamp: int
    details: dict
    location: Optional[Tuple[float, float]]

class LocationHistory:
    """Manages location history for fraud detection"""
    
    def __init__(self, max_history: int = 50):
        self.max_history = max_history
        self.user_locations: Dict[str, deque] = defaultdict(lambda: deque(maxlen=max_history))
    
    def add_location(self, user_id: str, event: LocationEvent):
        self.user_locations[user_id].append(event)
    
    def get_recent_locations(self, user_id: str, count: int = 10) -> List[LocationEvent]:
        locations = self.user_locations.get(user_id, deque())
        return list(locations)[-count:]

class RedisCache:
    """Redis cache for storing user states and counters"""
    
    def __init__(self):
        self.redis_client = redis.Redis(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            db=0,
            decode_responses=True
        )
    
    def increment_counter(self, key: str, window_seconds: int = 60) -> int:
        """Increment a time-windowed counter"""
        current_time = int(time.time())
        pipeline = self.redis_client.pipeline()
        
        # Use sliding window with multiple time buckets
        bucket = current_time // 10  # 10-second buckets
        window_key = f"{key}:{bucket}"
        
        pipeline.incr(window_key)
        pipeline.expire(window_key, window_seconds)
        
        # Count across the window
        window_buckets = window_seconds // 10
        total = 0
        for i in range(window_buckets):
            bucket_key = f"{key}:{bucket - i}"
            count = self.redis_client.get(bucket_key) or 0
            total += int(count)
        
        pipeline.execute()
        return total
    
    def set_user_state(self, user_id: str, state: dict, ttl: int = 3600):
        """Set user state with TTL"""
        self.redis_client.setex(f"user_state:{user_id}", ttl, json.dumps(state))
    
    def get_user_state(self, user_id: str) -> Optional[dict]:
        """Get user state"""
        data = self.redis_client.get(f"user_state:{user_id}")
        return json.loads(data) if data else None

class FraudDetector:
    """Real-time fraud detection engine"""
    
    def __init__(self):
        self.location_history = LocationHistory()
        self.redis_cache = RedisCache()
        self.alerts = []
        
        # Detection thresholds
        self.MAX_SPEED_KMH = 50  # Maximum human speed
        self.MAX_LOCATIONS_PER_MINUTE = 20  # Rapid location updates
        self.MAX_TILES_PER_MINUTE = 8  # Rapid exploration
        self.MIN_ACCURACY_METERS = 100  # Poor GPS accuracy
        self.TELEPORT_THRESHOLD_KM = 1  # Instant teleportation
        self.TELEPORT_TIME_SECONDS = 3
    
    def haversine_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate distance between two points in kilometers"""
        R = 6371  # Earth radius in kilometers
        
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        
        a = (math.sin(dlat / 2) * math.sin(dlat / 2) +
             math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
             math.sin(dlon / 2) * math.sin(dlon / 2))
        
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        
        return R * c
    
    def check_impossible_speed(self, user_id: str, current_event: LocationEvent) -> Optional[FraudAlert]:
        """Check for impossible movement speed"""
        recent_locations = self.location_history.get_recent_locations(user_id, 2)
        
        if len(recent_locations) < 1:
            return None
        
        last_event = recent_locations[-1]
        distance_km = self.haversine_distance(
            last_event.latitude, last_event.longitude,
            current_event.latitude, current_event.longitude
        )
        
        time_diff_seconds = (current_event.timestamp - last_event.timestamp) / 1000
        
        if time_diff_seconds <= 0:
            return None
        
        speed_kmh = (distance_km / time_diff_seconds) * 3600
        
        if speed_kmh > self.MAX_SPEED_KMH and time_diff_seconds > 5:
            return FraudAlert(
                user_id=user_id,
                alert_type="impossible_speed",
                risk_score=min(95, 50 + (speed_kmh - self.MAX_SPEED_KMH)),
                timestamp=current_event.timestamp,
                details={
                    "calculated_speed_kmh": speed_kmh,
                    "distance_km": distance_km,
                    "time_diff_seconds": time_diff_seconds,
                    "max_allowed_speed": self.MAX_SPEED_KMH
                },
                location=(current_event.latitude, current_event.longitude)
            )
        
        return None
    
    def check_teleportation(self, user_id: str, current_event: LocationEvent) -> Optional[FraudAlert]:
        """Check for instant teleportation"""
        recent_locations = self.location_history.get_recent_locations(user_id, 2)
        
        if len(recent_locations) < 1:
            return None
        
        last_event = recent_locations[-1]
        distance_km = self.haversine_distance(
            last_event.latitude, last_event.longitude,
            current_event.latitude, current_event.longitude
        )
        
        time_diff_seconds = (current_event.timestamp - last_event.timestamp) / 1000
        
        if distance_km > self.TELEPORT_THRESHOLD_KM and time_diff_seconds < self.TELEPORT_TIME_SECONDS:
            return FraudAlert(
                user_id=user_id,
                alert_type="teleportation",
                risk_score=90,
                timestamp=current_event.timestamp,
                details={
                    "distance_km": distance_km,
                    "time_diff_seconds": time_diff_seconds,
                    "threshold_km": self.TELEPORT_THRESHOLD_KM,
                    "threshold_seconds": self.TELEPORT_TIME_SECONDS
                },
                location=(current_event.latitude, current_event.longitude)
            )
        
        return None
    
    def check_rapid_updates(self, user_id: str) -> Optional[FraudAlert]:
        """Check for rapid location updates (bot behavior)"""
        counter_key = f"location_updates:{user_id}"
        update_count = self.redis_cache.increment_counter(counter_key, 60)
        
        if update_count > self.MAX_LOCATIONS_PER_MINUTE:
            return FraudAlert(
                user_id=user_id,
                alert_type="rapid_location_updates",
                risk_score=70,
                timestamp=int(time.time() * 1000),
                details={
                    "updates_per_minute": update_count,
                    "threshold": self.MAX_LOCATIONS_PER_MINUTE
                },
                location=None
            )
        
        return None
    
    def check_poor_accuracy(self, current_event: LocationEvent) -> Optional[FraudAlert]:
        """Check for poor GPS accuracy (potential spoofing)"""
        if current_event.accuracy > self.MIN_ACCURACY_METERS:
            return FraudAlert(
                user_id=current_event.user_id,
                alert_type="poor_gps_accuracy",
                risk_score=40,
                timestamp=current_event.timestamp,
                details={
                    "accuracy_meters": current_event.accuracy,
                    "threshold_meters": self.MIN_ACCURACY_METERS
                },
                location=(current_event.latitude, current_event.longitude)
            )
        
        return None
    
    def check_pattern_anomalies(self, user_id: str, current_event: LocationEvent) -> Optional[FraudAlert]:
        """Check for suspicious movement patterns"""
        recent_locations = self.location_history.get_recent_locations(user_id, 10)
        
        if len(recent_locations) < 5:
            return None
        
        # Check for identical coordinates (GPS spoofing indicator)
        identical_count = sum(1 for loc in recent_locations 
                            if abs(loc.latitude - current_event.latitude) < 0.000001 and
                               abs(loc.longitude - current_event.longitude) < 0.000001)
        
        if identical_count > 3:
            return FraudAlert(
                user_id=user_id,
                alert_type="identical_coordinates",
                risk_score=75,
                timestamp=current_event.timestamp,
                details={
                    "identical_count": identical_count,
                    "coordinate": f"{current_event.latitude},{current_event.longitude}"
                },
                location=(current_event.latitude, current_event.longitude)
            )
        
        # Check for perfectly regular timing (bot behavior)
        if len(recent_locations) >= 5:
            time_intervals = []
            for i in range(1, len(recent_locations)):
                interval = recent_locations[i].timestamp - recent_locations[i-1].timestamp
                time_intervals.append(interval)
            
            if time_intervals:
                avg_interval = sum(time_intervals) / len(time_intervals)
                variance = sum((interval - avg_interval) ** 2 for interval in time_intervals) / len(time_intervals)
                
                # Very low variance indicates robotic timing
                if variance < 1000 and avg_interval > 0:  # Less than 1 second variance
                    return FraudAlert(
                        user_id=user_id,
                        alert_type="robotic_timing",
                        risk_score=80,
                        timestamp=current_event.timestamp,
                        details={
                            "timing_variance": variance,
                            "average_interval_ms": avg_interval
                        },
                        location=(current_event.latitude, current_event.longitude)
                    )
        
        return None
    
    def analyze_location_event(self, event: LocationEvent) -> List[FraudAlert]:
        """Analyze a location event for fraud indicators"""
        alerts = []
        
        # Run all fraud checks
        checks = [
            self.check_impossible_speed(event.user_id, event),
            self.check_teleportation(event.user_id, event),
            self.check_rapid_updates(event.user_id),
            self.check_poor_accuracy(event),
            self.check_pattern_anomalies(event.user_id, event)
        ]
        
        # Add non-None alerts
        alerts.extend([alert for alert in checks if alert is not None])
        
        # Store location in history
        self.location_history.add_location(event.user_id, event)
        
        return alerts

class KinesisConsumer:
    """Kinesis stream consumer for real-time event processing"""
    
    def __init__(self, stream_name: str, region_name: str = 'us-east-1'):
        self.kinesis_client = boto3.client('kinesis', region_name=region_name)
        self.stream_name = stream_name
        self.fraud_detector = FraudDetector()
        self.alerting_service = AlertingService()
        
        # Get stream description
        response = self.kinesis_client.describe_stream(StreamName=stream_name)
        self.shards = response['StreamDescription']['Shards']
        
        logger.info(f"Initialized consumer for stream {stream_name} with {len(self.shards)} shards")
    
    def get_shard_iterator(self, shard_id: str) -> str:
        """Get shard iterator for latest records"""
        response = self.kinesis_client.get_shard_iterator(
            StreamName=self.stream_name,
            ShardId=shard_id,
            ShardIteratorType='LATEST'
        )
        return response['ShardIterator']
    
    def process_records(self, records: List[dict]):
        """Process a batch of Kinesis records"""
        for record in records:
            try:
                # Decode the record data
                data = json.loads(record['Data'])
                
                # Skip non-location events
                if data.get('event_type') != 'location_exploration':
                    continue
                
                # Create LocationEvent object
                location_event = LocationEvent(
                    user_id=data['user_id'],
                    timestamp=data['timestamp'],
                    latitude=data['location']['latitude'],
                    longitude=data['location']['longitude'],
                    accuracy=data['location']['accuracy'],
                    speed=data['location'].get('speed'),
                    platform=data['device_info']['platform'],
                    session_id=data['session_id'],
                    device_id=data['device_info']['device_id']
                )
                
                # Analyze for fraud
                alerts = self.fraud_detector.analyze_location_event(location_event)
                
                # Send alerts if any found
                for alert in alerts:
                    self.alerting_service.send_alert(alert)
                    logger.warning(f"Fraud alert: {alert.alert_type} for user {alert.user_id} (score: {alert.risk_score})")
                
            except Exception as e:
                logger.error(f"Error processing record: {e}")
    
    def consume_shard(self, shard_id: str):
        """Consume records from a single shard"""
        shard_iterator = self.get_shard_iterator(shard_id)
        
        while True:
            try:
                response = self.kinesis_client.get_records(
                    ShardIterator=shard_iterator,
                    Limit=100
                )
                
                records = response['Records']
                if records:
                    self.process_records(records)
                    logger.info(f"Processed {len(records)} records from shard {shard_id}")
                
                # Update shard iterator
                shard_iterator = response.get('NextShardIterator')
                if not shard_iterator:
                    logger.warning(f"No more records in shard {shard_id}")
                    break
                
                # Brief pause to avoid hitting limits
                time.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Error consuming shard {shard_id}: {e}")
                time.sleep(5)  # Wait before retrying
    
    def start_consuming(self):
        """Start consuming from all shards using thread pool"""
        logger.info("Starting Kinesis consumer...")
        
        with ThreadPoolExecutor(max_workers=len(self.shards)) as executor:
            futures = []
            for shard in self.shards:
                shard_id = shard['ShardId']
                future = executor.submit(self.consume_shard, shard_id)
                futures.append(future)
            
            # Wait for all consumers to complete (they run indefinitely)
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Shard consumer failed: {e}")

class AlertingService:
    """Service for sending fraud alerts"""
    
    def __init__(self):
        self.sns_client = boto3.client('sns')
        self.alert_topic_arn = os.getenv('FRAUD_ALERT_TOPIC_ARN')
        
        # High-risk users cache to avoid spam
        self.redis_cache = RedisCache()
    
    def send_alert(self, alert: FraudAlert):
        """Send fraud alert via SNS"""
        
        # Check if we've already alerted for this user recently
        alert_key = f"alert:{alert.user_id}:{alert.alert_type}"
        recent_alert = self.redis_cache.redis_client.get(alert_key)
        
        if recent_alert and alert.risk_score < 90:
            # Skip low-risk alerts if we've alerted recently
            return
        
        # Set alert cooldown (5 minutes for most alerts, 1 minute for high-risk)
        cooldown = 60 if alert.risk_score >= 90 else 300
        self.redis_cache.redis_client.setex(alert_key, cooldown, "1")
        
        # Create alert message
        message = {
            "alert_type": alert.alert_type,
            "user_id": alert.user_id,
            "risk_score": alert.risk_score,
            "timestamp": alert.timestamp,
            "details": alert.details,
            "location": alert.location,
            "severity": "HIGH" if alert.risk_score >= 80 else "MEDIUM" if alert.risk_score >= 60 else "LOW"
        }
        
        try:
            if self.alert_topic_arn:
                self.sns_client.publish(
                    TopicArn=self.alert_topic_arn,
                    Message=json.dumps(message, indent=2),
                    Subject=f"Fraud Alert: {alert.alert_type} (Score: {alert.risk_score})"
                )
                
                logger.info(f"Sent {alert.alert_type} alert for user {alert.user_id}")
            else:
                logger.warning("No SNS topic configured, logging alert instead")
                logger.warning(f"FRAUD ALERT: {json.dumps(message, indent=2)}")
                
        except Exception as e:
            logger.error(f"Failed to send alert: {e}")

def main():
    """Main function to start the fraud detection pipeline"""
    
    # Configuration from environment variables
    stream_name = os.getenv('KINESIS_STREAM_NAME', 'trumen-game-events')
    region = os.getenv('AWS_REGION', 'us-east-1')
    
    logger.info("Starting Real-time Fraud Detection Pipeline")
    
    try:
        # Initialize and start Kinesis consumer
        consumer = KinesisConsumer(stream_name, region)
        consumer.start_consuming()
        
    except KeyboardInterrupt:
        logger.info("Shutting down fraud detection pipeline...")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()