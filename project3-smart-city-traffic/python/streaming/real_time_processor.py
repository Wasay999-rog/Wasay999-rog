"""
Real-Time Traffic Data Processor
Advanced streaming analytics for smart city traffic management using Apache Kafka and Spark Streaming
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.streaming import StreamingContext
from kafka import KafkaConsumer, KafkaProducer
import redis
import joblib

# Machine Learning and Analytics
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import tensorflow as tf

# Custom imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from traffic_utils import TrafficAnalyzer, CongestionDetector, RouteOptimizer
from iot_utils import SensorDataValidator, AnomalyDetector
from config_manager import ConfigManager

class RealTimeTrafficProcessor:
    """
    Real-time traffic data processor for smart city analytics
    Handles streaming IoT data, applies ML models, and triggers actions
    """
    
    def __init__(self, config_path=None):
        self.config = ConfigManager(config_path)
        self.setup_logging()
        self.setup_connections()
        self.load_ml_models()
        self.initialize_analytics_engines()
        
        # Performance metrics
        self.processed_messages = 0
        self.processing_times = []
        self.anomalies_detected = 0
        
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
    def setup_connections(self):
        """Initialize connections to streaming and storage systems"""
        # Spark Session for stream processing
        self.spark = SparkSession.builder \
            .appName("SmartCityTrafficProcessor") \
            .config("spark.streaming.kafka.maxRatePerPartition", "1000") \
            .config("spark.sql.streaming.metricsEnabled", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        # Kafka configuration
        self.kafka_config = {
            'bootstrap_servers': self.config.get('kafka.bootstrap_servers', 'localhost:9092'),
            'value_deserializer': lambda m: json.loads(m.decode('utf-8')),
            'auto_offset_reset': 'latest',
            'enable_auto_commit': True,
            'group_id': 'traffic-processor-group'
        }
        
        # Redis for real-time caching
        self.redis_client = redis.Redis(
            host=self.config.get('redis.host', 'localhost'),
            port=self.config.get('redis.port', 6379),
            db=0,
            decode_responses=True
        )
        
        # Kafka producer for alerts and processed data
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_config['bootstrap_servers'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
    def load_ml_models(self):
        """Load pre-trained machine learning models"""
        model_path = self.config.get('models.path', 'ml_models/trained_models/')
        
        try:
            # Traffic prediction model
            self.traffic_predictor = joblib.load(
                os.path.join(model_path, 'traffic_prediction_model.pkl')
            )
            
            # Congestion detection model
            self.congestion_detector = joblib.load(
                os.path.join(model_path, 'congestion_detection_model.pkl')
            )
            
            # Anomaly detection model
            self.anomaly_detector = IsolationForest(
                contamination=0.1,
                random_state=42
            )
            
            # Load scalers
            self.scaler = joblib.load(
                os.path.join(model_path, 'feature_scaler.pkl')
            )
            
            self.logger.info("ML models loaded successfully")
            
        except Exception as e:
            self.logger.error(f"Error loading ML models: {e}")
            raise
    
    def initialize_analytics_engines(self):
        """Initialize analytics engines"""
        self.traffic_analyzer = TrafficAnalyzer()
        self.route_optimizer = RouteOptimizer()
        self.sensor_validator = SensorDataValidator()
        
    def define_schemas(self):
        """Define schemas for different data types"""
        # Traffic sensor schema
        self.traffic_schema = StructType([
            StructField("sensor_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("location", StructType([
                StructField("latitude", DoubleType(), True),
                StructField("longitude", DoubleType(), True),
                StructField("road_id", StringType(), True),
                StructField("lane_id", StringType(), True)
            ]), True),
            StructField("measurements", StructType([
                StructField("vehicle_count", IntegerType(), True),
                StructField("average_speed", DoubleType(), True),
                StructField("occupancy", DoubleType(), True),
                StructField("queue_length", DoubleType(), True)
            ]), True),
            StructField("vehicle_classification", MapType(StringType(), IntegerType()), True),
            StructField("weather_conditions", StructType([
                StructField("temperature", DoubleType(), True),
                StructField("humidity", DoubleType(), True),
                StructField("visibility", DoubleType(), True),
                StructField("precipitation", DoubleType(), True)
            ]), True)
        ])
        
        # Incident schema
        self.incident_schema = StructType([
            StructField("incident_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("location", StructType([
                StructField("latitude", DoubleType(), True),
                StructField("longitude", DoubleType(), True)
            ]), True),
            StructField("incident_type", StringType(), True),
            StructField("severity", StringType(), True),
            StructField("lanes_affected", IntegerType(), True),
            StructField("estimated_duration", IntegerType(), True)
        ])
        
    def process_traffic_stream(self):
        """Process real-time traffic data stream"""
        # Read from Kafka
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_config['bootstrap_servers']) \
            .option("subscribe", "traffic-sensors") \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse JSON data
        traffic_df = df.select(
            from_json(col("value").cast("string"), self.traffic_schema).alias("data")
        ).select("data.*")
        
        # Add processing timestamp
        traffic_df = traffic_df.withColumn("processing_time", current_timestamp())
        
        # Perform real-time analytics
        processed_df = self.apply_traffic_analytics(traffic_df)
        
        # Write to multiple sinks
        query = processed_df.writeStream \
            .foreachBatch(self.process_batch) \
            .outputMode("append") \
            .trigger(processingTime='10 seconds') \
            .option("checkpointLocation", "/tmp/checkpoint/traffic") \
            .start()
        
        return query
    
    def apply_traffic_analytics(self, df):
        """Apply analytics transformations to traffic data"""
        # Calculate traffic metrics
        df = df.withColumn(
            "traffic_density",
            col("measurements.vehicle_count") / col("measurements.occupancy")
        )
        
        df = df.withColumn(
            "congestion_level",
            when(col("measurements.average_speed") < 15, "Heavy")
            .when(col("measurements.average_speed") < 30, "Moderate")
            .when(col("measurements.average_speed") < 50, "Light")
            .otherwise("Free Flow")
        )
        
        # Add time-based features
        df = df.withColumn("hour", hour(col("timestamp"))) \
               .withColumn("day_of_week", dayofweek(col("timestamp"))) \
               .withColumn("is_weekend", when(dayofweek(col("timestamp")).isin([1, 7]), 1).otherwise(0))
        
        # Window-based aggregations for trend analysis
        df = df.withWatermark("timestamp", "5 minutes")
        
        return df
    
    def process_batch(self, df, batch_id):
        """Process each batch of streaming data"""
        start_time = time.time()
        
        try:
            # Convert to Pandas for ML processing
            pdf = df.toPandas()
            
            if len(pdf) == 0:
                return
            
            self.logger.info(f"Processing batch {batch_id} with {len(pdf)} records")
            
            # Apply ML models
            self.apply_ml_models(pdf)
            
            # Update real-time metrics
            self.update_real_time_metrics(pdf)
            
            # Detect and handle anomalies
            self.detect_anomalies(pdf)
            
            # Generate alerts if needed
            self.generate_alerts(pdf)
            
            # Store processed data
            self.store_processed_data(pdf)
            
            # Update performance metrics
            processing_time = time.time() - start_time
            self.processing_times.append(processing_time)
            self.processed_messages += len(pdf)
            
            self.logger.info(f"Batch {batch_id} processed in {processing_time:.2f} seconds")
            
        except Exception as e:
            self.logger.error(f"Error processing batch {batch_id}: {e}")
    
    def apply_ml_models(self, df):
        """Apply machine learning models to the data"""
        # Prepare features for ML models
        features = self.prepare_features(df)
        
        if len(features) == 0:
            return
        
        # Traffic prediction
        try:
            traffic_predictions = self.predict_traffic(features)
            df['predicted_traffic'] = traffic_predictions
            
            # Congestion detection
            congestion_predictions = self.detect_congestion(features)
            df['congestion_probability'] = congestion_predictions
            
            # Anomaly detection
            anomaly_scores = self.detect_sensor_anomalies(features)
            df['anomaly_score'] = anomaly_scores
            
        except Exception as e:
            self.logger.error(f"Error applying ML models: {e}")
    
    def prepare_features(self, df):
        """Prepare features for machine learning models"""
        try:
            # Extract relevant features
            features = []
            
            for _, row in df.iterrows():
                feature_vector = [
                    row.get('measurements', {}).get('vehicle_count', 0),
                    row.get('measurements', {}).get('average_speed', 0),
                    row.get('measurements', {}).get('occupancy', 0),
                    row.get('measurements', {}).get('queue_length', 0),
                    row.get('hour', 0),
                    row.get('day_of_week', 0),
                    row.get('is_weekend', 0),
                    row.get('weather_conditions', {}).get('temperature', 20),
                    row.get('weather_conditions', {}).get('humidity', 50),
                    row.get('weather_conditions', {}).get('precipitation', 0)
                ]
                features.append(feature_vector)
            
            # Convert to numpy array and scale
            features_array = np.array(features)
            if hasattr(self.scaler, 'transform'):
                features_scaled = self.scaler.transform(features_array)
                return features_scaled
            else:
                return features_array
                
        except Exception as e:
            self.logger.error(f"Error preparing features: {e}")
            return np.array([])
    
    def predict_traffic(self, features):
        """Predict future traffic conditions"""
        try:
            if hasattr(self.traffic_predictor, 'predict'):
                predictions = self.traffic_predictor.predict(features)
                return predictions.tolist()
            else:
                return [0] * len(features)
        except Exception as e:
            self.logger.error(f"Error in traffic prediction: {e}")
            return [0] * len(features)
    
    def detect_congestion(self, features):
        """Detect traffic congestion using ML model"""
        try:
            if hasattr(self.congestion_detector, 'predict_proba'):
                probabilities = self.congestion_detector.predict_proba(features)
                return probabilities[:, 1].tolist()  # Probability of congestion
            else:
                return [0] * len(features)
        except Exception as e:
            self.logger.error(f"Error in congestion detection: {e}")
            return [0] * len(features)
    
    def detect_sensor_anomalies(self, features):
        """Detect sensor data anomalies"""
        try:
            if len(features) > 0:
                # Fit anomaly detector if not already fitted
                if not hasattr(self.anomaly_detector, 'decision_function'):
                    self.anomaly_detector.fit(features)
                
                anomaly_scores = self.anomaly_detector.decision_function(features)
                return anomaly_scores.tolist()
            else:
                return []
        except Exception as e:
            self.logger.error(f"Error in anomaly detection: {e}")
            return [0] * len(features)
    
    def update_real_time_metrics(self, df):
        """Update real-time traffic metrics in Redis"""
        try:
            # Aggregate metrics by road segment
            road_metrics = df.groupby('location').agg({
                'measurements': lambda x: {
                    'avg_speed': np.mean([m.get('average_speed', 0) for m in x]),
                    'total_vehicles': np.sum([m.get('vehicle_count', 0) for m in x]),
                    'avg_occupancy': np.mean([m.get('occupancy', 0) for m in x])
                }
            }).to_dict()
            
            # Store in Redis with expiration
            for road_id, metrics in road_metrics.items():
                key = f"traffic:real_time:{road_id}"
                self.redis_client.setex(
                    key, 
                    timedelta(minutes=10),
                    json.dumps(metrics)
                )
                
            # Update city-wide metrics
            city_metrics = {
                'timestamp': datetime.now().isoformat(),
                'total_sensors': len(df),
                'avg_city_speed': df['measurements'].apply(
                    lambda x: x.get('average_speed', 0)
                ).mean(),
                'congestion_areas': len(df[df['congestion_level'] == 'Heavy'])
            }
            
            self.redis_client.setex(
                "traffic:city_metrics",
                timedelta(minutes=5),
                json.dumps(city_metrics)
            )
            
        except Exception as e:
            self.logger.error(f"Error updating real-time metrics: {e}")
    
    def detect_anomalies(self, df):
        """Detect and handle traffic anomalies"""
        try:
            # Check for sensor anomalies
            anomalies = df[df['anomaly_score'] < -0.5]  # Threshold for anomalies
            
            if len(anomalies) > 0:
                self.anomalies_detected += len(anomalies)
                
                for _, anomaly in anomalies.iterrows():
                    alert = {
                        'type': 'sensor_anomaly',
                        'sensor_id': anomaly['sensor_id'],
                        'timestamp': anomaly['timestamp'].isoformat() if pd.notna(anomaly['timestamp']) else None,
                        'location': anomaly['location'],
                        'anomaly_score': anomaly['anomaly_score'],
                        'measurements': anomaly['measurements']
                    }
                    
                    # Send alert to monitoring system
                    self.producer.send('traffic-alerts', alert)
                    
                    self.logger.warning(f"Sensor anomaly detected: {anomaly['sensor_id']}")
            
            # Check for traffic pattern anomalies
            speed_anomalies = df[df['measurements'].apply(
                lambda x: x.get('average_speed', 0) < 5 or x.get('average_speed', 0) > 120
            )]
            
            for _, speed_anomaly in speed_anomalies.iterrows():
                alert = {
                    'type': 'speed_anomaly',
                    'sensor_id': speed_anomaly['sensor_id'],
                    'timestamp': speed_anomaly['timestamp'].isoformat() if pd.notna(speed_anomaly['timestamp']) else None,
                    'location': speed_anomaly['location'],
                    'speed': speed_anomaly['measurements'].get('average_speed', 0)
                }
                
                self.producer.send('traffic-alerts', alert)
                
        except Exception as e:
            self.logger.error(f"Error in anomaly detection: {e}")
    
    def generate_alerts(self, df):
        """Generate traffic alerts based on conditions"""
        try:
            # High congestion alerts
            high_congestion = df[df['congestion_probability'] > 0.8]
            
            for _, congestion in high_congestion.iterrows():
                alert = {
                    'type': 'high_congestion',
                    'sensor_id': congestion['sensor_id'],
                    'timestamp': congestion['timestamp'].isoformat() if pd.notna(congestion['timestamp']) else None,
                    'location': congestion['location'],
                    'congestion_probability': congestion['congestion_probability'],
                    'current_speed': congestion['measurements'].get('average_speed', 0),
                    'recommendation': 'Consider alternative routes'
                }
                
                self.producer.send('traffic-alerts', alert)
                
            # Incident detection based on sudden speed drops
            speed_drops = df[
                df['measurements'].apply(lambda x: x.get('average_speed', 0)) < 
                df['predicted_traffic'] * 0.3  # 70% speed reduction from prediction
            ]
            
            for _, incident in speed_drops.iterrows():
                alert = {
                    'type': 'potential_incident',
                    'sensor_id': incident['sensor_id'],
                    'timestamp': incident['timestamp'].isoformat() if pd.notna(incident['timestamp']) else None,
                    'location': incident['location'],
                    'current_speed': incident['measurements'].get('average_speed', 0),
                    'predicted_speed': incident['predicted_traffic'],
                    'severity': 'high' if incident['measurements'].get('average_speed', 0) < 5 else 'medium'
                }
                
                self.producer.send('incident-alerts', alert)
                
        except Exception as e:
            self.logger.error(f"Error generating alerts: {e}")
    
    def store_processed_data(self, df):
        """Store processed data for historical analysis"""
        try:
            # Store in time-series database (simulated with file storage)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"processed_traffic_{timestamp}.json"
            
            # Convert to JSON format
            processed_data = df.to_dict('records')
            
            # In production, this would go to a time-series database like InfluxDB
            # For demo purposes, we'll store summary metrics
            summary_metrics = {
                'timestamp': datetime.now().isoformat(),
                'total_sensors': len(df),
                'avg_speed': df['measurements'].apply(lambda x: x.get('average_speed', 0)).mean(),
                'high_congestion_areas': len(df[df['congestion_probability'] > 0.7]),
                'anomalies': len(df[df['anomaly_score'] < -0.5])
            }
            
            # Store summary in Redis for dashboard
            self.redis_client.setex(
                f"traffic:summary:{timestamp}",
                timedelta(hours=24),
                json.dumps(summary_metrics)
            )
            
        except Exception as e:
            self.logger.error(f"Error storing processed data: {e}")
    
    def process_incident_stream(self):
        """Process incident reports stream"""
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_config['bootstrap_servers']) \
            .option("subscribe", "traffic-incidents") \
            .option("startingOffsets", "latest") \
            .load()
        
        incident_df = df.select(
            from_json(col("value").cast("string"), self.incident_schema).alias("data")
        ).select("data.*")
        
        # Process incidents
        query = incident_df.writeStream \
            .foreachBatch(self.process_incident_batch) \
            .outputMode("append") \
            .trigger(processingTime='5 seconds') \
            .option("checkpointLocation", "/tmp/checkpoint/incidents") \
            .start()
        
        return query
    
    def process_incident_batch(self, df, batch_id):
        """Process incident data batch"""
        try:
            pdf = df.toPandas()
            
            if len(pdf) == 0:
                return
            
            self.logger.info(f"Processing {len(pdf)} incidents in batch {batch_id}")
            
            # Update route recommendations based on incidents
            for _, incident in pdf.iterrows():
                self.route_optimizer.update_incident(incident.to_dict())
                
                # Generate route diversion alerts
                affected_routes = self.route_optimizer.find_affected_routes(
                    incident['location'], incident['lanes_affected']
                )
                
                for route in affected_routes:
                    alert = {
                        'type': 'route_diversion',
                        'incident_id': incident['incident_id'],
                        'affected_route': route,
                        'alternative_routes': self.route_optimizer.suggest_alternatives(route),
                        'estimated_delay': incident['estimated_duration']
                    }
                    
                    self.producer.send('route-alerts', alert)
            
        except Exception as e:
            self.logger.error(f"Error processing incident batch {batch_id}: {e}")
    
    def get_performance_metrics(self):
        """Get processor performance metrics"""
        return {
            'processed_messages': self.processed_messages,
            'average_processing_time': np.mean(self.processing_times) if self.processing_times else 0,
            'anomalies_detected': self.anomalies_detected,
            'uptime': time.time() - self.start_time if hasattr(self, 'start_time') else 0
        }
    
    def start_processing(self):
        """Start the real-time processing"""
        self.logger.info("Starting real-time traffic processor...")
        self.start_time = time.time()
        
        try:
            # Define schemas
            self.define_schemas()
            
            # Start traffic stream processing
            traffic_query = self.process_traffic_stream()
            
            # Start incident stream processing  
            incident_query = self.process_incident_stream()
            
            # Wait for streams to complete
            traffic_query.awaitTermination()
            incident_query.awaitTermination()
            
        except KeyboardInterrupt:
            self.logger.info("Stopping processor...")
            self.stop_processing()
        except Exception as e:
            self.logger.error(f"Error in processing: {e}")
            raise
    
    def stop_processing(self):
        """Stop the processor gracefully"""
        try:
            if hasattr(self, 'spark'):
                self.spark.stop()
            
            if hasattr(self, 'producer'):
                self.producer.close()
                
            self.logger.info("Processor stopped successfully")
            
        except Exception as e:
            self.logger.error(f"Error stopping processor: {e}")

def main():
    """Main function to run the real-time processor"""
    processor = RealTimeTrafficProcessor()
    
    try:
        processor.start_processing()
    except KeyboardInterrupt:
        print("\nStopping processor...")
        processor.stop_processing()
    except Exception as e:
        print(f"Error: {e}")
        processor.stop_processing()

if __name__ == "__main__":
    main()