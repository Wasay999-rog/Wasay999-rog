"""
Customer Segmentation Analysis for E-commerce
Implements RFM analysis, K-means clustering, and advanced customer segmentation
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score
from sklearn.decomposition import PCA
import warnings
warnings.filterwarnings('ignore')

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from database_connector import SnowflakeConnector
from config_manager import ConfigManager

class CustomerSegmentation:
    """
    Comprehensive customer segmentation analysis using RFM methodology
    and advanced machine learning clustering techniques.
    """
    
    def __init__(self):
        self.config = ConfigManager()
        self.db = SnowflakeConnector()
        self.customer_data = None
        self.rfm_data = None
        self.clusters = None
        
    def extract_customer_data(self):
        """Extract customer transaction data from Snowflake"""
        query = """
        SELECT 
            c.customer_id,
            c.first_name || ' ' || c.last_name as customer_name,
            c.email,
            c.registration_date,
            c.age,
            c.gender,
            c.city,
            c.state,
            c.country,
            COUNT(DISTINCT o.order_id) as total_orders,
            SUM(o.total_amount) as total_revenue,
            AVG(o.total_amount) as avg_order_value,
            MAX(o.order_date) as last_order_date,
            MIN(o.order_date) as first_order_date,
            DATEDIFF('day', MAX(o.order_date), CURRENT_DATE()) as days_since_last_order,
            DATEDIFF('day', MIN(o.order_date), MAX(o.order_date)) as customer_lifespan,
            COUNT(DISTINCT DATE_TRUNC('month', o.order_date)) as active_months
        FROM ECOMMERCE_DW.STAGING.stg_customers c
        LEFT JOIN ECOMMERCE_DW.RAW_DATA.orders o ON c.customer_id = o.customer_id
        WHERE o.order_status = 'completed'
        GROUP BY c.customer_id, c.first_name, c.last_name, c.email, 
                 c.registration_date, c.age, c.gender, c.city, c.state, c.country
        HAVING COUNT(o.order_id) > 0
        """
        
        self.customer_data = self.db.execute_query(query)
        print(f"Extracted {len(self.customer_data)} customers for analysis")
        return self.customer_data
    
    def calculate_rfm_scores(self):
        """Calculate RFM (Recency, Frequency, Monetary) scores"""
        if self.customer_data is None:
            self.extract_customer_data()
        
        # Calculate RFM metrics
        rfm = self.customer_data.copy()
        
        # Recency: Days since last purchase (lower is better)
        rfm['recency'] = rfm['days_since_last_order']
        
        # Frequency: Number of orders (higher is better)
        rfm['frequency'] = rfm['total_orders']
        
        # Monetary: Total revenue (higher is better)
        rfm['monetary'] = rfm['total_revenue']
        
        # Calculate RFM scores (1-5 scale)
        rfm['recency_score'] = pd.qcut(rfm['recency'].rank(method='first'), 
                                      q=5, labels=[5,4,3,2,1])
        rfm['frequency_score'] = pd.qcut(rfm['frequency'].rank(method='first'), 
                                        q=5, labels=[1,2,3,4,5])
        rfm['monetary_score'] = pd.qcut(rfm['monetary'].rank(method='first'), 
                                       q=5, labels=[1,2,3,4,5])
        
        # Create RFM segment
        rfm['rfm_score'] = (rfm['recency_score'].astype(str) + 
                           rfm['frequency_score'].astype(str) + 
                           rfm['monetary_score'].astype(str))
        
        # Define customer segments based on RFM scores
        def segment_customers(row):
            if row['rfm_score'] in ['555', '554', '544', '545', '454', '455', '445']:
                return 'Champions'
            elif row['rfm_score'] in ['543', '444', '435', '355', '354', '345', '344', '335']:
                return 'Loyal Customers'
            elif row['rfm_score'] in ['512', '511', '422', '421', '412', '411', '311']:
                return 'Potential Loyalists'
            elif row['rfm_score'] in ['534', '343', '334', '343', '334', '325', '324']:
                return 'New Customers'
            elif row['rfm_score'] in ['155', '154', '144', '214', '215', '115', '114']:
                return 'At Risk'
            elif row['rfm_score'] in ['155', '154', '144', '214', '215', '115']:
                return 'Cannot Lose Them'
            elif row['rfm_score'] in ['332', '322', '231', '241', '251', '233', '232']:
                return 'Need Attention'
            else:
                return 'Lost'
        
        rfm['segment'] = rfm.apply(segment_customers, axis=1)
        
        self.rfm_data = rfm
        return rfm
    
    def perform_kmeans_clustering(self, n_clusters=None):
        """Perform K-means clustering on customer features"""
        if self.rfm_data is None:
            self.calculate_rfm_scores()
        
        # Prepare features for clustering
        features = ['recency', 'frequency', 'monetary', 'avg_order_value', 
                   'customer_lifespan', 'active_months']
        
        X = self.rfm_data[features].fillna(0)
        
        # Standardize features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        # Determine optimal number of clusters if not provided
        if n_clusters is None:
            n_clusters = self.find_optimal_clusters(X_scaled)
        
        # Perform K-means clustering
        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        cluster_labels = kmeans.fit_predict(X_scaled)
        
        # Add cluster labels to data
        self.rfm_data['cluster'] = cluster_labels
        self.rfm_data['cluster_name'] = self.rfm_data['cluster'].map(
            self.get_cluster_names(n_clusters)
        )
        
        # Calculate cluster characteristics
        cluster_summary = self.rfm_data.groupby('cluster').agg({
            'recency': 'mean',
            'frequency': 'mean', 
            'monetary': 'mean',
            'avg_order_value': 'mean',
            'customer_lifespan': 'mean',
            'customer_id': 'count'
        }).round(2)
        
        cluster_summary.columns = ['Avg_Recency', 'Avg_Frequency', 'Avg_Monetary', 
                                  'Avg_Order_Value', 'Avg_Lifespan', 'Customer_Count']
        
        print("Cluster Summary:")
        print(cluster_summary)
        
        return cluster_labels, cluster_summary
    
    def find_optimal_clusters(self, X_scaled, max_clusters=10):
        """Find optimal number of clusters using elbow method and silhouette score"""
        inertias = []
        silhouette_scores = []
        
        K_range = range(2, max_clusters + 1)
        
        for k in K_range:
            kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
            kmeans.fit(X_scaled)
            inertias.append(kmeans.inertia_)
            silhouette_scores.append(silhouette_score(X_scaled, kmeans.labels_))
        
        # Find elbow point
        optimal_k = K_range[np.argmax(silhouette_scores)]
        
        print(f"Optimal number of clusters: {optimal_k}")
        print(f"Silhouette score: {max(silhouette_scores):.3f}")
        
        return optimal_k
    
    def get_cluster_names(self, n_clusters):
        """Generate descriptive names for clusters"""
        if n_clusters == 4:
            return {
                0: 'Price Sensitive',
                1: 'High Value',
                2: 'Regular Customers', 
                3: 'New Customers'
            }
        elif n_clusters == 5:
            return {
                0: 'Champions',
                1: 'Loyal Customers',
                2: 'At Risk',
                3: 'New Customers',
                4: 'Lost Customers'
            }
        else:
            return {i: f'Cluster_{i}' for i in range(n_clusters)}
    
    def create_visualizations(self):
        """Create comprehensive visualizations for customer segmentation"""
        if self.rfm_data is None:
            self.calculate_rfm_scores()
        
        # Set up the plotting style
        plt.style.use('seaborn-v0_8')
        
        # 1. RFM Distribution
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Customer Segmentation Analysis', fontsize=16, fontweight='bold')
        
        # RFM Scores Distribution
        self.rfm_data['recency_score'].value_counts().plot(kind='bar', ax=axes[0,0], color='skyblue')
        axes[0,0].set_title('Recency Score Distribution')
        axes[0,0].set_xlabel('Recency Score')
        axes[0,0].set_ylabel('Number of Customers')
        
        self.rfm_data['frequency_score'].value_counts().plot(kind='bar', ax=axes[0,1], color='lightgreen')
        axes[0,1].set_title('Frequency Score Distribution')
        axes[0,1].set_xlabel('Frequency Score')
        axes[0,1].set_ylabel('Number of Customers')
        
        self.rfm_data['monetary_score'].value_counts().plot(kind='bar', ax=axes[1,0], color='salmon')
        axes[1,0].set_title('Monetary Score Distribution')
        axes[1,0].set_xlabel('Monetary Score')
        axes[1,0].set_ylabel('Number of Customers')
        
        # Segment Distribution
        segment_counts = self.rfm_data['segment'].value_counts()
        axes[1,1].pie(segment_counts.values, labels=segment_counts.index, autopct='%1.1f%%', startangle=90)
        axes[1,1].set_title('Customer Segments Distribution')
        
        plt.tight_layout()
        plt.savefig('customer_segmentation_overview.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        # 2. Cluster Analysis Visualization
        if 'cluster' in self.rfm_data.columns:
            self.create_cluster_visualizations()
        
        # 3. Interactive Plotly Visualizations
        self.create_interactive_visualizations()
    
    def create_cluster_visualizations(self):
        """Create cluster-specific visualizations"""
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('K-Means Clustering Analysis', fontsize=16, fontweight='bold')
        
        # Cluster distribution
        cluster_counts = self.rfm_data['cluster_name'].value_counts()
        axes[0,0].pie(cluster_counts.values, labels=cluster_counts.index, autopct='%1.1f%%', startangle=90)
        axes[0,0].set_title('Cluster Distribution')
        
        # Recency vs Frequency by cluster
        for cluster in self.rfm_data['cluster'].unique():
            cluster_data = self.rfm_data[self.rfm_data['cluster'] == cluster]
            axes[0,1].scatter(cluster_data['recency'], cluster_data['frequency'], 
                            label=f'Cluster {cluster}', alpha=0.7)
        axes[0,1].set_xlabel('Recency (days)')
        axes[0,1].set_ylabel('Frequency (orders)')
        axes[0,1].set_title('Recency vs Frequency by Cluster')
        axes[0,1].legend()
        
        # Frequency vs Monetary by cluster
        for cluster in self.rfm_data['cluster'].unique():
            cluster_data = self.rfm_data[self.rfm_data['cluster'] == cluster]
            axes[1,0].scatter(cluster_data['frequency'], cluster_data['monetary'], 
                            label=f'Cluster {cluster}', alpha=0.7)
        axes[1,0].set_xlabel('Frequency (orders)')
        axes[1,0].set_ylabel('Monetary (revenue)')
        axes[1,0].set_title('Frequency vs Monetary by Cluster')
        axes[1,0].legend()
        
        # Average metrics by cluster
        cluster_metrics = self.rfm_data.groupby('cluster_name')[['recency', 'frequency', 'monetary']].mean()
        cluster_metrics.plot(kind='bar', ax=axes[1,1], rot=45)
        axes[1,1].set_title('Average RFM Metrics by Cluster')
        axes[1,1].set_xlabel('Cluster')
        axes[1,1].set_ylabel('Average Value')
        axes[1,1].legend()
        
        plt.tight_layout()
        plt.savefig('cluster_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()
    
    def create_interactive_visualizations(self):
        """Create interactive Plotly visualizations"""
        # 3D scatter plot of RFM
        fig = px.scatter_3d(
            self.rfm_data, 
            x='recency', 
            y='frequency', 
            z='monetary',
            color='segment',
            size='avg_order_value',
            hover_data=['customer_name', 'total_orders'],
            title='3D RFM Analysis by Customer Segment'
        )
        fig.write_html('rfm_3d_analysis.html')
        fig.show()
        
        # Customer segment metrics
        segment_metrics = self.rfm_data.groupby('segment').agg({
            'customer_id': 'count',
            'monetary': 'sum',
            'avg_order_value': 'mean',
            'frequency': 'mean'
        }).reset_index()
        
        fig = px.sunburst(
            self.rfm_data,
            path=['segment', 'gender', 'age'],
            values='monetary',
            title='Customer Segmentation Hierarchy'
        )
        fig.write_html('segment_hierarchy.html')
        fig.show()
    
    def generate_segment_insights(self):
        """Generate actionable insights for each customer segment"""
        if self.rfm_data is None:
            self.calculate_rfm_scores()
        
        insights = {}
        
        for segment in self.rfm_data['segment'].unique():
            segment_data = self.rfm_data[self.rfm_data['segment'] == segment]
            
            insights[segment] = {
                'customer_count': len(segment_data),
                'avg_recency': segment_data['recency'].mean(),
                'avg_frequency': segment_data['frequency'].mean(),
                'avg_monetary': segment_data['monetary'].mean(),
                'avg_order_value': segment_data['avg_order_value'].mean(),
                'total_revenue': segment_data['monetary'].sum(),
                'revenue_percentage': (segment_data['monetary'].sum() / 
                                     self.rfm_data['monetary'].sum() * 100)
            }
        
        # Generate recommendations
        recommendations = {
            'Champions': 'Reward with exclusive offers, early access to new products',
            'Loyal Customers': 'Up-sell and cross-sell, referral programs',
            'Potential Loyalists': 'Membership programs, personalized recommendations',
            'New Customers': 'Welcome campaigns, onboarding programs',
            'At Risk': 'Win-back campaigns, special discounts',
            'Cannot Lose Them': 'Dedicated account manager, premium support',
            'Need Attention': 'Re-engagement campaigns, surveys for feedback',
            'Lost': 'Aggressive win-back campaigns, exit surveys'
        }
        
        # Print insights
        print("\n" + "="*60)
        print("CUSTOMER SEGMENT INSIGHTS AND RECOMMENDATIONS")
        print("="*60)
        
        for segment, data in insights.items():
            print(f"\n{segment.upper()}")
            print("-" * len(segment))
            print(f"Customer Count: {data['customer_count']:,}")
            print(f"Avg Recency: {data['avg_recency']:.1f} days")
            print(f"Avg Frequency: {data['avg_frequency']:.1f} orders")
            print(f"Avg Monetary: ${data['avg_monetary']:,.2f}")
            print(f"Avg Order Value: ${data['avg_order_value']:,.2f}")
            print(f"Total Revenue: ${data['total_revenue']:,.2f}")
            print(f"Revenue %: {data['revenue_percentage']:.1f}%")
            if segment in recommendations:
                print(f"Recommendation: {recommendations[segment]}")
        
        return insights, recommendations
    
    def export_results(self, filename='customer_segmentation_results.xlsx'):
        """Export segmentation results to Excel"""
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Customer data with segments
            self.rfm_data.to_excel(writer, sheet_name='Customer_Segments', index=False)
            
            # Segment summary
            segment_summary = self.rfm_data.groupby('segment').agg({
                'customer_id': 'count',
                'recency': 'mean',
                'frequency': 'mean',
                'monetary': ['mean', 'sum'],
                'avg_order_value': 'mean'
            }).round(2)
            segment_summary.to_excel(writer, sheet_name='Segment_Summary')
            
            # RFM score distribution
            rfm_distribution = pd.crosstab([self.rfm_data['recency_score'], 
                                          self.rfm_data['frequency_score']], 
                                         self.rfm_data['monetary_score'])
            rfm_distribution.to_excel(writer, sheet_name='RFM_Distribution')
        
        print(f"Results exported to {filename}")

def main():
    """Main execution function"""
    # Initialize customer segmentation analysis
    cs = CustomerSegmentation()
    
    # Extract and analyze customer data
    print("Extracting customer data...")
    cs.extract_customer_data()
    
    print("Calculating RFM scores...")
    cs.calculate_rfm_scores()
    
    print("Performing K-means clustering...")
    cs.perform_kmeans_clustering()
    
    print("Creating visualizations...")
    cs.create_visualizations()
    
    print("Generating insights...")
    cs.generate_segment_insights()
    
    print("Exporting results...")
    cs.export_results()
    
    print("\nCustomer segmentation analysis completed successfully!")

if __name__ == "__main__":
    main()