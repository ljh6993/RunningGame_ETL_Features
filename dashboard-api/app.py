"""
Real-time Analytics Dashboard API for TrumenApp
Connects to Apache Druid for near real-time feature usage dashboards and A/B test analysis
"""

from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import requests
import json
from datetime import datetime, timedelta
import os
from typing import Dict, List, Any, Optional
import logging
from functools import wraps
import redis
import hashlib

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Configuration
DRUID_BROKER_URL = os.getenv('DRUID_BROKER_URL', 'http://localhost:8082')
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
CACHE_TTL = int(os.getenv('CACHE_TTL', 300))  # 5 minutes

# Initialize Redis cache
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

def cache_result(ttl: int = CACHE_TTL):
    """Decorator to cache API results"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            # Create cache key from function name and arguments
            cache_key = f"{f.__name__}:{hashlib.md5(str(sorted(request.args.items())).encode()).hexdigest()}"
            
            try:
                # Try to get from cache
                cached_result = redis_client.get(cache_key)
                if cached_result:
                    logger.info(f"Cache hit for {cache_key}")
                    return json.loads(cached_result)
            except Exception as e:
                logger.warning(f"Cache error: {e}")
            
            # Execute function
            result = f(*args, **kwargs)
            
            try:
                # Store in cache
                redis_client.setex(cache_key, ttl, json.dumps(result, default=str))
                logger.info(f"Cached result for {cache_key}")
            except Exception as e:
                logger.warning(f"Cache storage error: {e}")
            
            return result
        return decorated_function
    return decorator

class DruidQueryBuilder:
    """Helper class to build Druid queries"""
    
    @staticmethod
    def build_timeseries_query(
        datasource: str,
        start_time: str,
        end_time: str,
        granularity: str = "hour",
        aggregations: List[Dict] = None,
        filters: List[Dict] = None,
        dimensions: List[str] = None
    ) -> Dict[str, Any]:
        """Build a Druid timeseries query"""
        
        query = {
            "queryType": "timeseries",
            "dataSource": datasource,
            "intervals": [f"{start_time}/{end_time}"],
            "granularity": granularity,
            "aggregations": aggregations or [
                {"type": "count", "name": "events"},
                {"type": "hyperUnique", "name": "unique_users", "fieldName": "user_id"}
            ],
            "context": {"timeout": 60000}
        }
        
        if filters:
            query["filter"] = {"type": "and", "fields": filters}
        
        if dimensions:
            query["dimensions"] = dimensions
            
        return query
    
    @staticmethod
    def build_topn_query(
        datasource: str,
        start_time: str,
        end_time: str,
        dimension: str,
        metric: str,
        threshold: int = 10,
        filters: List[Dict] = None
    ) -> Dict[str, Any]:
        """Build a Druid TopN query"""
        
        query = {
            "queryType": "topN",
            "dataSource": datasource,
            "intervals": [f"{start_time}/{end_time}"],
            "granularity": "all",
            "dimension": dimension,
            "metric": metric,
            "threshold": threshold,
            "aggregations": [
                {"type": "count", "name": "events"},
                {"type": "hyperUnique", "name": "unique_users", "fieldName": "user_id"}
            ],
            "context": {"timeout": 60000}
        }
        
        if filters:
            query["filter"] = {"type": "and", "fields": filters}
            
        return query

def execute_druid_query(query: Dict[str, Any]) -> Dict[str, Any]:
    """Execute a Druid query and return results"""
    try:
        response = requests.post(
            f"{DRUID_BROKER_URL}/druid/v2",
            json=query,
            headers={"Content-Type": "application/json"},
            timeout=60
        )
        response.raise_for_status()
        return {"success": True, "data": response.json()}
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Druid query failed: {e}")
        return {"success": False, "error": str(e)}

# Dashboard API Endpoints

@app.route('/')
def dashboard():
    """Main dashboard page"""
    return render_template('dashboard.html')

@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    try:
        # Check Druid connection
        response = requests.get(f"{DRUID_BROKER_URL}/status/health", timeout=5)
        druid_healthy = response.status_code == 200
    except:
        druid_healthy = False
    
    try:
        # Check Redis connection
        redis_client.ping()
        redis_healthy = True
    except:
        redis_healthy = False
    
    return jsonify({
        "status": "healthy" if druid_healthy and redis_healthy else "unhealthy",
        "druid": "healthy" if druid_healthy else "unhealthy",
        "redis": "healthy" if redis_healthy else "unhealthy",
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/overview')
@cache_result(ttl=60)  # Cache for 1 minute
def get_overview_metrics():
    """Get overview metrics for the dashboard"""
    
    # Get time range (last 24 hours)
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=24)
    
    start_iso = start_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    end_iso = end_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    
    metrics = {}
    
    # Daily Active Users
    dau_query = DruidQueryBuilder.build_timeseries_query(
        datasource="location_events",
        start_time=start_iso,
        end_time=end_iso,
        granularity="day",
        aggregations=[
            {"type": "hyperUnique", "name": "unique_users", "fieldName": "user_id"}
        ]
    )
    
    dau_result = execute_druid_query(dau_query)
    if dau_result["success"]:
        dau_data = dau_result["data"]
        metrics["daily_active_users"] = dau_data[0]["result"]["unique_users"] if dau_data else 0
    
    # Total Events
    events_query = DruidQueryBuilder.build_timeseries_query(
        datasource="location_events",
        start_time=start_iso,
        end_time=end_iso,
        granularity="day",
        aggregations=[
            {"type": "count", "name": "total_events"}
        ]
    )
    
    events_result = execute_druid_query(events_query)
    if events_result["success"]:
        events_data = events_result["data"]
        metrics["total_events"] = events_data[0]["result"]["total_events"] if events_data else 0
    
    # Platform Distribution
    platform_query = DruidQueryBuilder.build_topn_query(
        datasource="location_events",
        start_time=start_iso,
        end_time=end_iso,
        dimension="platform",
        metric="unique_users",
        threshold=10
    )
    
    platform_result = execute_druid_query(platform_query)
    if platform_result["success"]:
        metrics["platform_distribution"] = platform_result["data"]
    
    # Fraud Alerts (last 24 hours)
    fraud_query = DruidQueryBuilder.build_timeseries_query(
        datasource="suspicious_activities",
        start_time=start_iso,
        end_time=end_iso,
        granularity="day",
        aggregations=[
            {"type": "count", "name": "fraud_alerts"},
            {"type": "doubleSum", "name": "avg_risk_score", "fieldName": "risk_score"}
        ]
    )
    
    fraud_result = execute_druid_query(fraud_query)
    if fraud_result["success"]:
        fraud_data = fraud_result["data"]
        metrics["fraud_alerts"] = fraud_data[0]["result"]["fraud_alerts"] if fraud_data else 0
        metrics["avg_risk_score"] = fraud_data[0]["result"]["avg_risk_score"] if fraud_data else 0
    
    return jsonify(metrics)

@app.route('/api/user-activity')
@cache_result(ttl=300)  # Cache for 5 minutes
def get_user_activity():
    """Get user activity trends over time"""
    
    hours = request.args.get('hours', 24, type=int)
    granularity = request.args.get('granularity', 'hour')
    
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=hours)
    
    start_iso = start_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    end_iso = end_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    
    # User activity over time
    activity_query = DruidQueryBuilder.build_timeseries_query(
        datasource="location_events",
        start_time=start_iso,
        end_time=end_iso,
        granularity=granularity,
        aggregations=[
            {"type": "count", "name": "events"},
            {"type": "hyperUnique", "name": "active_users", "fieldName": "user_id"},
            {"type": "longSum", "name": "tiles_explored", "fieldName": "tile_count"},
            {"type": "doubleSum", "name": "distance_covered", "fieldName": "movement_distance"}
        ]
    )
    
    result = execute_druid_query(activity_query)
    
    if result["success"]:
        return jsonify(result["data"])
    else:
        return jsonify({"error": result["error"]}), 500

@app.route('/api/feature-usage')
@cache_result(ttl=300)
def get_feature_usage():
    """Get feature usage analytics"""
    
    hours = request.args.get('hours', 24, type=int)
    
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=hours)
    
    start_iso = start_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    end_iso = end_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    
    # Top actions
    actions_query = DruidQueryBuilder.build_topn_query(
        datasource="user_actions",
        start_time=start_iso,
        end_time=end_iso,
        dimension="action_type",
        metric="events",
        threshold=20
    )
    
    result = execute_druid_query(actions_query)
    
    if result["success"]:
        return jsonify(result["data"])
    else:
        return jsonify({"error": result["error"]}), 500

@app.route('/api/fraud-monitoring')
@cache_result(ttl=60)  # Cache for 1 minute (fraud data should be fresh)
def get_fraud_monitoring():
    """Get fraud detection monitoring data"""
    
    hours = request.args.get('hours', 24, type=int)
    
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=hours)
    
    start_iso = start_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    end_iso = end_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    
    results = {}
    
    # Fraud alerts over time
    alerts_query = DruidQueryBuilder.build_timeseries_query(
        datasource="suspicious_activities",
        start_time=start_iso,
        end_time=end_iso,
        granularity="hour",
        aggregations=[
            {"type": "count", "name": "alerts"},
            {"type": "hyperUnique", "name": "flagged_users", "fieldName": "user_id"},
            {"type": "doubleMax", "name": "max_risk_score", "fieldName": "risk_score"},
            {"type": "doubleSum", "name": "total_risk", "fieldName": "risk_score"}
        ]
    )
    
    alerts_result = execute_druid_query(alerts_query)
    if alerts_result["success"]:
        results["alerts_timeline"] = alerts_result["data"]
    
    # Top suspicious activity types
    types_query = DruidQueryBuilder.build_topn_query(
        datasource="suspicious_activities",
        start_time=start_iso,
        end_time=end_iso,
        dimension="suspicious_type",
        metric="alerts",
        threshold=10
    )
    
    types_result = execute_druid_query(types_query)
    if types_result["success"]:
        results["suspicious_types"] = types_result["data"]
    
    # High-risk users
    users_query = DruidQueryBuilder.build_topn_query(
        datasource="suspicious_activities",
        start_time=start_iso,
        end_time=end_iso,
        dimension="user_id",
        metric="total_risk",
        threshold=20,
        filters=[
            {"type": "bound", "dimension": "risk_score", "lower": "70", "lowerStrict": False}
        ]
    )
    
    users_result = execute_druid_query(users_query)
    if users_result["success"]:
        results["high_risk_users"] = users_result["data"]
    
    return jsonify(results)

@app.route('/api/ab-test/<test_name>')
@cache_result(ttl=300)
def get_ab_test_results(test_name: str):
    """Get A/B test analysis results"""
    
    days = request.args.get('days', 7, type=int)
    
    end_time = datetime.now()
    start_time = end_time - timedelta(days=days)
    
    start_iso = start_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    end_iso = end_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    
    # A/B test performance by variant
    test_query = DruidQueryBuilder.build_timeseries_query(
        datasource="user_actions",
        start_time=start_iso,
        end_time=end_iso,
        granularity="day",
        aggregations=[
            {"type": "count", "name": "events"},
            {"type": "hyperUnique", "name": "unique_users", "fieldName": "user_id"},
            {"type": "longSum", "name": "conversions", "fieldName": "conversion_count"}
        ],
        filters=[
            {"type": "selector", "dimension": "ab_test_name", "value": test_name}
        ],
        dimensions=["ab_test_variant"]
    )
    
    result = execute_druid_query(test_query)
    
    if result["success"]:
        return jsonify(result["data"])
    else:
        return jsonify({"error": result["error"]}), 500

@app.route('/api/geographic-heatmap')
@cache_result(ttl=600)  # Cache for 10 minutes
def get_geographic_heatmap():
    """Get geographic distribution of user activity"""
    
    hours = request.args.get('hours', 24, type=int)
    
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=hours)
    
    start_iso = start_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    end_iso = end_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    
    # Geographic aggregation (simplified - would need proper geo-hashing in production)
    geo_query = {
        "queryType": "select",
        "dataSource": "location_events",
        "intervals": [f"{start_iso}/{end_iso}"],
        "dimensions": ["user_id", "latitude", "longitude", "platform"],
        "metrics": ["__count"],
        "granularity": "all",
        "pagingSpec": {"pagingIdentifiers": {}, "threshold": 1000},
        "context": {"timeout": 60000}
    }
    
    result = execute_druid_query(geo_query)
    
    if result["success"]:
        # Process results to create heatmap data
        events = result["data"][0]["events"] if result["data"] else []
        heatmap_data = []
        
        for event in events:
            heatmap_data.append({
                "lat": event["latitude"],
                "lng": event["longitude"],
                "weight": 1,
                "platform": event["platform"]
            })
        
        return jsonify({"heatmap_data": heatmap_data})
    else:
        return jsonify({"error": result["error"]}), 500

if __name__ == '__main__':
    app.run(
        host='0.0.0.0',
        port=int(os.getenv('PORT', 5000)),
        debug=os.getenv('DEBUG', 'False').lower() == 'true'
    )