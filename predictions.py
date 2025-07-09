import shap
import json
import joblib
import pandas as pd
import time
from kafka import KafkaProducer

# Load the model and data
model = joblib.load('linear_regression_model.joblib')
data = pd.read_csv('new.csv')

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
model_features = ['week_of_year','is_holiday','is_weekend','season','discount_percent','price_per_unit','storeid','productid','prev_week_sales']  # <- adjust this list
data_for_model = data[model_features]
# Initialize SHAP explainer once (TreeExplainer or KernelExplainer depending on model type)
explainer = shap.Explainer(model.predict, data_for_model)

feature_names = data_for_model.columns.tolist()
coefficients = dict(zip(feature_names, model.coef_.tolist()))

# Loop through each row, once every 5 minutes
for idx, row in data_for_model.iterrows():
    row_df = row.to_frame().T  # Convert Series to DataFrame
    # Predict
    prediction = model.predict(row_df)[0]
    store_id = row['storeid']
    product_id = row['productid']
    # Compute SHAP values
    shap_values = explainer(row_df)

    # Prepare message
    shap_info = {
        'row_index': int(idx),
        'prediction': prediction,
        'features': dict(zip(row.index.tolist(), shap_values[0].values.tolist())),
        'base_value': float(shap_values[0].base_values),
        'coefficients': coefficients  # global explanation
    }

    # Send message
    producer.send('shap_values', shap_info)
    print(f"Sent SHAP data for row {idx}.")

    producer.send('predicted_sales', {
    'store_id': store_id,
    'product_id': product_id,
    'prediction': prediction
    })

    # Wait 5 minutes
    time.sleep(2)
