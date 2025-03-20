from flask import Flask, request, jsonify
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Get database connection string from environment
postgres_uri = os.environ.get('POSTGRES_URI', 'postgresql://postgres:postgres@localhost:5432/smartwatch_analytics')

def get_db_connection():
    """Create a database connection"""
    conn = psycopg2.connect(postgres_uri)
    return conn

@app.route('/api/user/<user_id>/steps/daily?days=30', methods=['GET'])
def get_user_daily_steps(user_id):
    """Retrieve a user’s daily steps summary for the past month"""
    try:
        days = int(request.args.get('days', 30))
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)
        
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        query = """
            SELECT sef.EventDate AS date, SUM(sef.TotalSteps) AS total_steps
            FROM SmartWatchEventFacts sef
            JOIN userDim ud ON sef.DeviceID = ud.device_id
            WHERE ud.user_id = %s AND sef.EventDate BETWEEN %s AND %s
            GROUP BY sef.EventDate
            ORDER BY sef.EventDate;
        """
        
        cursor.execute(query, (user_id, start_date, end_date))
        results = cursor.fetchall()
        
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


@app.route('/api/analytics/health-metrics/by-age', methods=['GET'])
def get_health_metrics_by_age():
    """Get average health metrics grouped into custom age groups"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        query = """
            SELECT 
                CASE 
                    WHEN u.age BETWEEN 18 AND 29 THEN '18-29'
                    WHEN u.age BETWEEN 30 AND 39 THEN '30-39'
                    WHEN u.age BETWEEN 40 AND 49 THEN '40-49'
                    WHEN u.age BETWEEN 50 AND 60 THEN '50-60'
                    ELSE 'Unknown' 
                END AS age_group,
                COUNT(DISTINCT u.user_id) AS user_count,
                AVG(sef.HeartRate) AS avg_heart_rate
            FROM 
                userDim u
            JOIN 
                SmartWatchEventFacts sef ON u.device_id = sef.DeviceID
            WHERE 
                sef.EventDate BETWEEN '2024-08-06' AND '2024-08-08'
            GROUP BY 
                age_group
            ORDER BY 
                age_group;
        """

        cursor.execute(query)
        results = cursor.fetchall()

        data = [dict(row) for row in results]

        cursor.close()
        conn.close()

        return jsonify({"data": data})

    except Exception as e:
        logger.error(f"Error retrieving analytics data: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == '__main__':
    logger.info("Starting query API server on port 5000...")
    app.run(host='0.0.0.0', port=5000)