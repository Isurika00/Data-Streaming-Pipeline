from flask import Flask, request, jsonify
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta
import logging
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Get database connection string from environment
postgres_uri = os.environ.get('POSTGRES_URI', 'postgresql://postgres:postgres@localhost:5432/smartwatch_analytics')

def get_db_connection():
    """Create a database connection"""
    conn = psycopg2.connect(postgres_uri)
    return conn

@app.route('/api/user/<user_id>/steps/daily', methods=['GET'])
def get_user_daily_steps(user_id):
    """Get daily steps for a user over the past month"""
    try:
        # Calculate date range (default to past 30 days)
        days = int(request.args.get('days', 30))
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)
        
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        query = """
            SELECT date, total_steps
            FROM daily_metrics
            WHERE user_id = %s AND date BETWEEN %s AND %s
            ORDER BY date
        """
        
        cursor.execute(query, (user_id, start_date, end_date))
        results = cursor.fetchall()
        
        # Convert to list of dicts
        data = [dict(row) for row in results]
        
        cursor.close()
        conn.close()
        
        return jsonify({
            "user_id": user_id,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "data": data
        })
        
    except Exception as e:
        logger.error(f"Error retrieving steps data: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/user/<user_id>/heart-rate/sleep', methods=['GET'])
def get_user_sleep_heart_rate(user_id):
    """Get sleep heart rate statistics for a user"""
    try:
        # Calculate date range (default to past 30 days)
        days = int(request.args.get('days', 30))
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)
        
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        query = """
            SELECT date, avg_sleep_heart_rate
            FROM daily_metrics
            WHERE user_id = %s 
              AND date BETWEEN %s AND %s
              AND avg_sleep_heart_rate IS NOT NULL
            ORDER BY date
        """
        
        cursor.execute(query, (user_id, start_date, end_date))
        results = cursor.fetchall()
        
        # Convert to list of dicts
        data = [dict(row) for row in results]
        
        cursor.close()
        conn.close()
        
        return jsonify({
            "user_id": user_id,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "data": data
        })
        
    except Exception as e:
        logger.error(f"Error retrieving heart rate data: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/analytics/heart-rate/by-age', methods=['GET'])
def get_heart_rate_by_age():
    """Get average heart rate grouped by age"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # Get age ranges in 10-year increments
        query = """
            SELECT 
                (FLOOR(u.age / 10) * 10) as age_group,
                CONCAT((FLOOR(u.age / 10) * 10), '-', (FLOOR(u.age / 10) * 10 + 9)) as age_range,
                AVG(dm.avg_heart_rate) as avg_heart_rate,
                AVG(dm.avg_sleep_heart_rate) as avg_sleep_heart_rate,
                COUNT(DISTINCT u.user_id) as user_count
            FROM 
                users u
            JOIN 
                daily_metrics dm ON u.user_id = dm.user_id
            WHERE 
                dm.date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY 
                age_group
            ORDER BY 
                age_group
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to list of dicts
        data = [dict(row) for row in results]
        
        cursor.close()
        conn.close()
        
        return jsonify({
            "data": data
        })
        
    except Exception as e:
        logger.error(f"Error retrieving analytics data: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    logger.info("Starting query API server on port 5000...")
    app.run(host='0.0.0.0', port=5000)