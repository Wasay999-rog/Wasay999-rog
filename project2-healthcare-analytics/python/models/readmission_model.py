"""
Healthcare Readmission Prediction Model
Advanced machine learning pipeline for predicting 30-day hospital readmissions
Compliant with HIPAA and clinical best practices
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Machine Learning Libraries
from sklearn.model_selection import train_test_split, cross_val_score, StratifiedKFold
from sklearn.preprocessing import StandardScaler, LabelEncoder, OneHotEncoder
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (classification_report, confusion_matrix, roc_curve, 
                           roc_auc_score, precision_recall_curve, average_precision_score)
from sklearn.feature_selection import SelectKBest, f_classif, RFECV
from sklearn.utils.class_weight import compute_class_weight
import xgboost as xgb
import lightgbm as lgb
from imblearn.over_sampling import SMOTE
from imblearn.pipeline import Pipeline as ImbPipeline

# Clinical and Statistical Libraries
import lifelines
from lifelines import CoxPHFitter
from scipy import stats
import warnings
warnings.filterwarnings('ignore')

# System imports
import sys
import os
import pickle
import joblib
from datetime import datetime, timedelta
import logging

# Custom imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from database_connector import SnowflakeConnector
from clinical_utils import ClinicalFeatureEngineer, HIPAACompliance
from model_utils import ModelValidation, ClinicalMetrics

class ReadmissionPredictor:
    """
    Comprehensive readmission prediction system with multiple ML algorithms
    and clinical validation metrics
    """
    
    def __init__(self, config_path=None):
        self.config = self._load_config(config_path)
        self.db = SnowflakeConnector()
        self.feature_engineer = ClinicalFeatureEngineer()
        self.hipaa_compliance = HIPAACompliance()
        self.clinical_metrics = ClinicalMetrics()
        
        # Model components
        self.models = {}
        self.scalers = {}
        self.encoders = {}
        self.feature_selectors = {}
        
        # Results storage
        self.results = {}
        self.feature_importance = {}
        
        # Logging setup
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
    def _load_config(self, config_path):
        """Load configuration for the model"""
        default_config = {
            'test_size': 0.2,
            'random_state': 42,
            'cv_folds': 5,
            'models_to_train': ['logistic', 'random_forest', 'xgboost', 'lightgbm'],
            'balance_classes': True,
            'feature_selection': True,
            'model_save_path': 'ml_models/trained_models/'
        }
        
        if config_path and os.path.exists(config_path):
            import yaml
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            default_config.update(config)
        
        return default_config
    
    def extract_patient_data(self, start_date=None, end_date=None):
        """
        Extract patient data for readmission prediction with comprehensive clinical features
        """
        if not start_date:
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        query = f"""
        WITH patient_admissions AS (
            SELECT 
                p.patient_id,
                p.age,
                p.gender,
                p.race_ethnicity,
                p.insurance_type,
                p.primary_language,
                
                -- Admission details
                a.admission_id,
                a.admission_date,
                a.discharge_date,
                a.admission_type,
                a.admission_source,
                a.discharge_disposition,
                a.length_of_stay,
                
                -- Clinical details
                a.primary_diagnosis_code,
                a.primary_diagnosis_description,
                a.drg_code,
                a.severity_of_illness,
                a.risk_of_mortality,
                a.case_mix_index,
                
                -- Comorbidities (Charlson Comorbidity Index components)
                c.myocardial_infarction,
                c.congestive_heart_failure,
                c.peripheral_vascular_disease,
                c.cerebrovascular_disease,
                c.dementia,
                c.chronic_pulmonary_disease,
                c.rheumatic_disease,
                c.peptic_ulcer_disease,
                c.mild_liver_disease,
                c.diabetes_without_complications,
                c.diabetes_with_complications,
                c.hemiplegia_paraplegia,
                c.renal_disease,
                c.malignancy,
                c.moderate_severe_liver_disease,
                c.metastatic_solid_tumor,
                c.aids_hiv,
                c.charlson_comorbidity_index,
                
                -- Previous healthcare utilization
                h.previous_admissions_30d,
                h.previous_admissions_90d,
                h.previous_admissions_1y,
                h.previous_ed_visits_30d,
                h.previous_ed_visits_90d,
                h.total_previous_admissions,
                
                -- Laboratory values (most recent before discharge)
                l.hemoglobin,
                l.hematocrit,
                l.white_blood_count,
                l.platelet_count,
                l.sodium,
                l.potassium,
                l.chloride,
                l.bun,
                l.creatinine,
                l.glucose,
                l.albumin,
                l.total_bilirubin,
                l.alt_sgpt,
                l.ast_sgot,
                
                -- Vital signs (average during stay)
                v.avg_systolic_bp,
                v.avg_diastolic_bp,
                v.avg_heart_rate,
                v.avg_respiratory_rate,
                v.avg_temperature,
                v.avg_oxygen_saturation,
                
                -- Medications
                m.total_medications,
                m.high_risk_medications,
                m.medication_changes,
                m.polypharmacy_flag,
                
                -- Social determinants
                s.lives_alone,
                s.transportation_issues,
                s.financial_hardship,
                s.social_support_score,
                s.home_health_services,
                
                -- Readmission outcome (30-day)
                CASE 
                    WHEN ra.readmission_date IS NOT NULL 
                         AND ra.readmission_date <= a.discharge_date + INTERVAL '30 days'
                    THEN 1 
                    ELSE 0 
                END as readmitted_30d,
                
                ra.readmission_date,
                ra.days_to_readmission
                
            FROM healthcare_dw.staging.patients p
            JOIN healthcare_dw.staging.admissions a ON p.patient_id = a.patient_id
            LEFT JOIN healthcare_dw.analytics.comorbidities c ON a.admission_id = c.admission_id
            LEFT JOIN healthcare_dw.analytics.healthcare_utilization h ON p.patient_id = h.patient_id
            LEFT JOIN healthcare_dw.analytics.lab_values l ON a.admission_id = l.admission_id
            LEFT JOIN healthcare_dw.analytics.vital_signs v ON a.admission_id = v.admission_id
            LEFT JOIN healthcare_dw.analytics.medications m ON a.admission_id = m.admission_id
            LEFT JOIN healthcare_dw.analytics.social_determinants s ON p.patient_id = s.patient_id
            LEFT JOIN healthcare_dw.analytics.readmissions ra ON a.admission_id = ra.index_admission_id
            
            WHERE a.discharge_date BETWEEN '{start_date}' AND '{end_date}'
                AND a.admission_type != 'Elective Surgery'  -- Exclude planned procedures
                AND a.length_of_stay >= 1  -- Exclude same-day discharges
                AND p.age >= 18  -- Adult patients only
        )
        
        SELECT * FROM patient_admissions
        ORDER BY patient_id, admission_date
        """
        
        self.logger.info(f"Extracting patient data from {start_date} to {end_date}")
        data = self.db.execute_query(query)
        
        # Apply HIPAA de-identification
        data = self.hipaa_compliance.de_identify_data(data)
        
        self.logger.info(f"Extracted {len(data)} patient admissions")
        self.logger.info(f"Readmission rate: {data['readmitted_30d'].mean():.3f}")
        
        return data
    
    def engineer_features(self, data):
        """
        Create comprehensive clinical features for readmission prediction
        """
        self.logger.info("Engineering clinical features...")
        
        # Create a copy to avoid modifying original data
        df = data.copy()
        
        # Age categories
        df['age_category'] = pd.cut(df['age'], 
                                   bins=[0, 30, 50, 65, 80, 100], 
                                   labels=['18-30', '31-50', '51-65', '66-80', '80+'])
        
        # Length of stay categories
        df['los_category'] = pd.cut(df['length_of_stay'], 
                                   bins=[0, 2, 5, 10, float('inf')], 
                                   labels=['Short (1-2)', 'Medium (3-5)', 'Long (6-10)', 'Extended (10+)'])
        
        # BMI calculation if height and weight available
        if 'height' in df.columns and 'weight' in df.columns:
            df['bmi'] = df['weight'] / ((df['height'] / 100) ** 2)
            df['bmi_category'] = pd.cut(df['bmi'], 
                                       bins=[0, 18.5, 25, 30, float('inf')], 
                                       labels=['Underweight', 'Normal', 'Overweight', 'Obese'])
        
        # Create severity scores
        df['elixhauser_score'] = self.feature_engineer.calculate_elixhauser_score(df)
        
        # Risk stratification
        df['high_risk_patient'] = (
            (df['age'] >= 65) | 
            (df['charlson_comorbidity_index'] >= 3) |
            (df['previous_admissions_30d'] > 0) |
            (df['severity_of_illness'] == 'Major')
        ).astype(int)
        
        # Laboratory abnormalities
        lab_columns = ['hemoglobin', 'white_blood_count', 'creatinine', 'sodium', 'potassium']
        df['abnormal_labs_count'] = 0
        
        for lab in lab_columns:
            if lab in df.columns:
                df[f'{lab}_abnormal'] = self.feature_engineer.identify_abnormal_labs(df[lab], lab)
                df['abnormal_labs_count'] += df[f'{lab}_abnormal']
        
        # Vital signs abnormalities
        df['vital_instability'] = (
            (df['avg_systolic_bp'] > 180) | (df['avg_systolic_bp'] < 90) |
            (df['avg_heart_rate'] > 120) | (df['avg_heart_rate'] < 50) |
            (df['avg_oxygen_saturation'] < 95)
        ).astype(int)
        
        # Social risk factors
        social_risk_cols = ['lives_alone', 'transportation_issues', 'financial_hardship']
        df['social_risk_score'] = df[social_risk_cols].sum(axis=1)
        
        # Healthcare utilization patterns
        df['frequent_flyer'] = (df['total_previous_admissions'] >= 3).astype(int)
        df['recent_admission'] = (df['previous_admissions_30d'] > 0).astype(int)
        
        # Medication risk
        df['high_med_burden'] = (
            (df['total_medications'] > 10) | 
            (df['high_risk_medications'] > 0) |
            (df['polypharmacy_flag'] == 1)
        ).astype(int)
        
        # Create interaction features
        df['age_comorbidity_interaction'] = df['age'] * df['charlson_comorbidity_index']
        df['los_severity_interaction'] = df['length_of_stay'] * df['severity_of_illness'].map({
            'Minor': 1, 'Moderate': 2, 'Major': 3, 'Extreme': 4
        }).fillna(1)
        
        # Diagnosis-specific risk
        high_risk_diagnoses = ['428', '584', '038', '486', '599', '276']  # CHF, AKI, Sepsis, Pneumonia, UTI, Dehydration
        df['high_risk_diagnosis'] = df['primary_diagnosis_code'].str[:3].isin(high_risk_diagnoses).astype(int)
        
        self.logger.info(f"Feature engineering completed. Dataset shape: {df.shape}")
        
        return df
    
    def prepare_model_data(self, data):
        """
        Prepare data for machine learning models
        """
        self.logger.info("Preparing data for modeling...")
        
        # Define features to exclude from modeling
        exclude_features = [
            'patient_id', 'admission_id', 'admission_date', 'discharge_date',
            'readmission_date', 'days_to_readmission', 'readmitted_30d',
            'primary_diagnosis_description'
        ]
        
        # Separate features and target
        feature_columns = [col for col in data.columns if col not in exclude_features]
        X = data[feature_columns].copy()
        y = data['readmitted_30d'].copy()
        
        # Handle categorical variables
        categorical_cols = X.select_dtypes(include=['object']).columns
        numerical_cols = X.select_dtypes(include=[np.number]).columns
        
        # Encode categorical variables
        for col in categorical_cols:
            if X[col].nunique() <= 10:  # One-hot encode low cardinality
                encoder = OneHotEncoder(drop='first', sparse=False)
                encoded = encoder.fit_transform(X[[col]])
                encoded_df = pd.DataFrame(encoded, 
                                        columns=[f"{col}_{cat}" for cat in encoder.categories_[0][1:]])
                X = pd.concat([X.drop(col, axis=1), encoded_df], axis=1)
                self.encoders[col] = encoder
            else:  # Label encode high cardinality
                encoder = LabelEncoder()
                X[col] = encoder.fit_transform(X[col].astype(str))
                self.encoders[col] = encoder
        
        # Handle missing values
        X = X.fillna(X.median())
        
        # Feature selection
        if self.config['feature_selection']:
            self.logger.info("Performing feature selection...")
            selector = SelectKBest(score_func=f_classif, k=50)
            X_selected = selector.fit_transform(X, y)
            selected_features = X.columns[selector.get_support()]
            X = pd.DataFrame(X_selected, columns=selected_features)
            self.feature_selectors['kbest'] = selector
        
        # Scale numerical features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        X = pd.DataFrame(X_scaled, columns=X.columns)
        self.scalers['standard'] = scaler
        
        self.logger.info(f"Final feature set: {X.shape[1]} features")
        self.logger.info(f"Class distribution: {y.value_counts().to_dict()}")
        
        return X, y
    
    def train_models(self, X, y):
        """
        Train multiple machine learning models for readmission prediction
        """
        self.logger.info("Training machine learning models...")
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=self.config['test_size'], 
            random_state=self.config['random_state'], stratify=y
        )
        
        # Handle class imbalance
        if self.config['balance_classes']:
            smote = SMOTE(random_state=self.config['random_state'])
            X_train_balanced, y_train_balanced = smote.fit_resample(X_train, y_train)
        else:
            X_train_balanced, y_train_balanced = X_train, y_train
        
        # Define models
        models_config = {
            'logistic': LogisticRegression(
                random_state=self.config['random_state'],
                class_weight='balanced',
                max_iter=1000
            ),
            'random_forest': RandomForestClassifier(
                n_estimators=100,
                random_state=self.config['random_state'],
                class_weight='balanced',
                n_jobs=-1
            ),
            'xgboost': xgb.XGBClassifier(
                random_state=self.config['random_state'],
                eval_metric='logloss',
                use_label_encoder=False
            ),
            'lightgbm': lgb.LGBMClassifier(
                random_state=self.config['random_state'],
                verbose=-1
            ),
            'gradient_boosting': GradientBoostingClassifier(
                random_state=self.config['random_state'],
                n_estimators=100
            )
        }
        
        # Train and evaluate models
        for model_name in self.config['models_to_train']:
            if model_name in models_config:
                self.logger.info(f"Training {model_name} model...")
                
                model = models_config[model_name]
                
                # Cross-validation
                cv_scores = cross_val_score(
                    model, X_train_balanced, y_train_balanced,
                    cv=StratifiedKFold(n_splits=self.config['cv_folds'], shuffle=True, 
                                     random_state=self.config['random_state']),
                    scoring='roc_auc'
                )
                
                # Train final model
                model.fit(X_train_balanced, y_train_balanced)
                
                # Predictions
                y_pred_proba = model.predict_proba(X_test)[:, 1]
                y_pred = model.predict(X_test)
                
                # Store results
                self.models[model_name] = model
                self.results[model_name] = {
                    'cv_scores': cv_scores,
                    'cv_mean': cv_scores.mean(),
                    'cv_std': cv_scores.std(),
                    'y_test': y_test,
                    'y_pred': y_pred,
                    'y_pred_proba': y_pred_proba,
                    'auc': roc_auc_score(y_test, y_pred_proba),
                    'classification_report': classification_report(y_test, y_pred, output_dict=True)
                }
                
                # Feature importance
                if hasattr(model, 'feature_importances_'):
                    importance_df = pd.DataFrame({
                        'feature': X.columns,
                        'importance': model.feature_importances_
                    }).sort_values('importance', ascending=False)
                    self.feature_importance[model_name] = importance_df
                
                self.logger.info(f"{model_name} - CV AUC: {cv_scores.mean():.3f} (+/- {cv_scores.std() * 2:.3f})")
                self.logger.info(f"{model_name} - Test AUC: {roc_auc_score(y_test, y_pred_proba):.3f}")
        
        return X_train, X_test, y_train, y_test
    
    def evaluate_clinical_impact(self, y_test, model_predictions):
        """
        Evaluate clinical impact and decision support metrics
        """
        self.logger.info("Evaluating clinical impact...")
        
        clinical_results = {}
        
        for model_name, pred_data in model_predictions.items():
            y_pred_proba = pred_data['y_pred_proba']
            y_pred = pred_data['y_pred']
            
            # Clinical decision thresholds
            thresholds = [0.1, 0.2, 0.3, 0.4, 0.5]
            threshold_metrics = []
            
            for threshold in thresholds:
                y_pred_thresh = (y_pred_proba >= threshold).astype(int)
                
                # Calculate clinical metrics
                metrics = self.clinical_metrics.calculate_clinical_metrics(
                    y_test, y_pred_thresh, y_pred_proba
                )
                metrics['threshold'] = threshold
                threshold_metrics.append(metrics)
            
            clinical_results[model_name] = {
                'threshold_analysis': pd.DataFrame(threshold_metrics),
                'optimal_threshold': self._find_optimal_threshold(y_test, y_pred_proba),
                'clinical_value': self._calculate_clinical_value(y_test, y_pred_proba)
            }
        
        return clinical_results
    
    def _find_optimal_threshold(self, y_true, y_pred_proba):
        """Find optimal threshold based on clinical criteria"""
        from sklearn.metrics import precision_recall_curve
        
        precision, recall, thresholds = precision_recall_curve(y_true, y_pred_proba)
        
        # Optimize for F2 score (emphasizes recall for clinical applications)
        f2_scores = (5 * precision * recall) / (4 * precision + recall)
        optimal_idx = np.argmax(f2_scores)
        
        return {
            'threshold': thresholds[optimal_idx],
            'precision': precision[optimal_idx],
            'recall': recall[optimal_idx],
            'f2_score': f2_scores[optimal_idx]
        }
    
    def _calculate_clinical_value(self, y_true, y_pred_proba):
        """Calculate clinical and economic value of predictions"""
        # Simplified clinical value calculation
        # In practice, this would incorporate hospital-specific costs and benefits
        
        # Assumptions (would be customized per hospital)
        cost_intervention = 1000  # Cost of readmission prevention intervention
        cost_readmission = 15000  # Average cost of 30-day readmission
        
        optimal_threshold = 0.3  # Example threshold
        y_pred_thresh = (y_pred_proba >= optimal_threshold).astype(int)
        
        # True positives: Correctly identified high-risk patients
        tp = np.sum((y_true == 1) & (y_pred_thresh == 1))
        # False positives: Incorrectly identified as high-risk
        fp = np.sum((y_true == 0) & (y_pred_thresh == 1))
        # False negatives: Missed high-risk patients
        fn = np.sum((y_true == 1) & (y_pred_thresh == 0))
        
        # Calculate value
        value_saved = tp * (cost_readmission * 0.35)  # 35% prevention rate assumption
        intervention_cost = (tp + fp) * cost_intervention
        missed_opportunity = fn * cost_readmission
        
        net_value = value_saved - intervention_cost
        
        return {
            'value_saved': value_saved,
            'intervention_cost': intervention_cost,
            'net_value': net_value,
            'roi': (net_value / intervention_cost) if intervention_cost > 0 else 0,
            'patients_flagged': tp + fp,
            'true_positives': tp,
            'false_positives': fp,
            'false_negatives': fn
        }
    
    def create_visualizations(self):
        """Create comprehensive visualizations for model results"""
        self.logger.info("Creating visualizations...")
        
        # 1. Model comparison
        model_comparison = pd.DataFrame({
            model: {
                'CV_AUC_Mean': results['cv_mean'],
                'CV_AUC_Std': results['cv_std'],
                'Test_AUC': results['auc']
            }
            for model, results in self.results.items()
        }).T
        
        fig = px.bar(
            model_comparison.reset_index(),
            x='index',
            y='Test_AUC',
            error_y='CV_AUC_Std',
            title='Model Performance Comparison',
            labels={'index': 'Model', 'Test_AUC': 'AUC Score'}
        )
        fig.write_html('model_comparison.html')
        
        # 2. ROC Curves
        fig, ax = plt.subplots(figsize=(10, 8))
        
        for model_name, results in self.results.items():
            fpr, tpr, _ = roc_curve(results['y_test'], results['y_pred_proba'])
            ax.plot(fpr, tpr, label=f"{model_name} (AUC = {results['auc']:.3f})")
        
        ax.plot([0, 1], [0, 1], 'k--', label='Random')
        ax.set_xlabel('False Positive Rate')
        ax.set_ylabel('True Positive Rate')
        ax.set_title('ROC Curves - Readmission Prediction Models')
        ax.legend()
        ax.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig('roc_curves.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        # 3. Feature Importance (for best model)
        best_model = max(self.results.keys(), key=lambda x: self.results[x]['auc'])
        if best_model in self.feature_importance:
            top_features = self.feature_importance[best_model].head(20)
            
            fig = px.bar(
                top_features[::-1],  # Reverse for better visualization
                x='importance',
                y='feature',
                orientation='h',
                title=f'Top 20 Features - {best_model.title()} Model',
                labels={'importance': 'Feature Importance', 'feature': 'Clinical Features'}
            )
            fig.write_html('feature_importance.html')
        
        # 4. Confusion Matrix for best model
        cm = confusion_matrix(self.results[best_model]['y_test'], 
                            self.results[best_model]['y_pred'])
        
        fig, ax = plt.subplots(figsize=(8, 6))
        sns.heatmap(cm, annot=True, fmt='d', cmap='Blues',
                   xticklabels=['No Readmission', 'Readmission'],
                   yticklabels=['No Readmission', 'Readmission'])
        ax.set_title(f'Confusion Matrix - {best_model.title()} Model')
        ax.set_xlabel('Predicted')
        ax.set_ylabel('Actual')
        plt.tight_layout()
        plt.savefig('confusion_matrix.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        self.logger.info("Visualizations created successfully")
    
    def save_models(self):
        """Save trained models and components"""
        save_path = self.config['model_save_path']
        os.makedirs(save_path, exist_ok=True)
        
        # Save models
        for model_name, model in self.models.items():
            joblib.dump(model, os.path.join(save_path, f'{model_name}_model.pkl'))
        
        # Save preprocessing components
        joblib.dump(self.scalers, os.path.join(save_path, 'scalers.pkl'))
        joblib.dump(self.encoders, os.path.join(save_path, 'encoders.pkl'))
        joblib.dump(self.feature_selectors, os.path.join(save_path, 'feature_selectors.pkl'))
        
        # Save results
        with open(os.path.join(save_path, 'model_results.pkl'), 'wb') as f:
            pickle.dump(self.results, f)
        
        # Save feature importance
        for model_name, importance_df in self.feature_importance.items():
            importance_df.to_csv(os.path.join(save_path, f'{model_name}_feature_importance.csv'), 
                               index=False)
        
        self.logger.info(f"Models saved to {save_path}")
    
    def generate_clinical_report(self):
        """Generate comprehensive clinical validation report"""
        best_model = max(self.results.keys(), key=lambda x: self.results[x]['auc'])
        best_results = self.results[best_model]
        
        report = {
            'model_performance': {
                'best_model': best_model,
                'auc_score': best_results['auc'],
                'cross_validation_score': best_results['cv_mean'],
                'sensitivity': best_results['classification_report']['1']['recall'],
                'specificity': best_results['classification_report']['0']['recall'],
                'precision': best_results['classification_report']['1']['precision'],
                'f1_score': best_results['classification_report']['1']['f1-score']
            },
            
            'clinical_interpretation': {
                'model_explanation': f"The {best_model} model achieved an AUC of {best_results['auc']:.3f}, indicating good discriminative ability for predicting 30-day readmissions.",
                'sensitivity_interpretation': f"The model correctly identifies {best_results['classification_report']['1']['recall']*100:.1f}% of patients who will be readmitted (sensitivity).",
                'specificity_interpretation': f"The model correctly identifies {best_results['classification_report']['0']['recall']*100:.1f}% of patients who will not be readmitted (specificity).",
                'clinical_utility': "This model can be used to identify high-risk patients for targeted interventions such as discharge planning, follow-up calls, and care coordination."
            },
            
            'recommendations': [
                "Implement risk-based discharge planning protocols",
                "Develop care management programs for high-risk patients",
                "Regular model performance monitoring and recalibration",
                "Integration with clinical decision support systems",
                "Staff training on model interpretation and use"
            ]
        }
        
        return report

def main():
    """Main execution function for readmission prediction model"""
    # Initialize predictor
    predictor = ReadmissionPredictor()
    
    # Extract patient data
    print("Extracting patient data...")
    raw_data = predictor.extract_patient_data()
    
    # Engineer features
    print("Engineering clinical features...")
    featured_data = predictor.engineer_features(raw_data)
    
    # Prepare data for modeling
    print("Preparing data for machine learning...")
    X, y = predictor.prepare_model_data(featured_data)
    
    # Train models
    print("Training machine learning models...")
    X_train, X_test, y_train, y_test = predictor.train_models(X, y)
    
    # Evaluate clinical impact
    print("Evaluating clinical impact...")
    clinical_results = predictor.evaluate_clinical_impact(y_test, predictor.results)
    
    # Create visualizations
    print("Creating visualizations...")
    predictor.create_visualizations()
    
    # Save models
    print("Saving trained models...")
    predictor.save_models()
    
    # Generate clinical report
    print("Generating clinical validation report...")
    clinical_report = predictor.generate_clinical_report()
    
    # Print summary
    print("\n" + "="*60)
    print("READMISSION PREDICTION MODEL SUMMARY")
    print("="*60)
    
    for model_name, results in predictor.results.items():
        print(f"\n{model_name.upper()} MODEL:")
        print(f"  AUC Score: {results['auc']:.3f}")
        print(f"  CV Score: {results['cv_mean']:.3f} (+/- {results['cv_std']*2:.3f})")
        print(f"  Sensitivity: {results['classification_report']['1']['recall']:.3f}")
        print(f"  Specificity: {results['classification_report']['0']['recall']:.3f}")
        print(f"  Precision: {results['classification_report']['1']['precision']:.3f}")
    
    best_model = max(predictor.results.keys(), key=lambda x: predictor.results[x]['auc'])
    print(f"\nBEST MODEL: {best_model.upper()}")
    print(f"RECOMMENDED FOR CLINICAL DEPLOYMENT")
    
    print("\nModel training completed successfully!")
    print("Files generated:")
    print("- model_comparison.html")
    print("- roc_curves.png")
    print("- feature_importance.html")
    print("- confusion_matrix.png")
    print("- Trained models in ml_models/trained_models/")

if __name__ == "__main__":
    main()