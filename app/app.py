

from math import exp
from flask import Flask, render_template, request, jsonify
import pandas as pd
import plotly.express as px
import plotly
import json
import os
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

app = Flask(__name__)

# Initialize spark and model as None
spark = None
model = None

def create_spark_session():
    global spark
    if spark is None:
        try:
            spark = SparkSession.builder \
                .appName("FoodPriceApp") \
                .config("spark.executor.memory", "2g") \
                .config("spark.driver.memory", "2g") \
                .getOrCreate()
            print("Spark session initialized successfully")
        except Exception as e:
            print(f"Error initializing Spark session: {str(e)}")
    return spark

def load_model():
    global model
    model_path = "../model/log_price_pipeline_model"
    if model is None:
        try:
            spark = create_spark_session()  # Ensure Spark session exists
            if os.path.exists(model_path):
                model = PipelineModel.load(model_path)
                print("Model loaded successfully")
            else:
                print(f"Error: Model not found at {os.path.abspath(model_path)}")
        except Exception as e:
            print(f"Error loading model: {str(e)}")
    return model

# Load data and initialize at startup
try:
    county_data = pd.read_csv("../data/county_region.csv")
    food_df = pd.read_csv("../data/commodity_category_price.csv")  # Fixed typo in filename
    rainfall_data = pd.read_csv("../data/monthly_rainfall.csv")

    # Prepare dropdown options
    regions = sorted(county_data['region'].dropna().unique())
    counties = sorted(county_data['county'].dropna().unique())
    markets = sorted(county_data['market'].dropna().unique())
    categories = sorted(food_df['category'].dropna().unique())
    commodities = sorted(food_df['commodity'].dropna().unique())  # Fixed column name to match your data

except Exception as e:
    print(f"Error loading data files: {str(e)}")
    regions = counties = markets = categories = commodities = []

months = [
    {'value': 1, 'name': 'January'}, {'value': 2, 'name': 'February'},
    {'value': 3, 'name': 'March'}, {'value': 4, 'name': 'April'},
    {'value': 5, 'name': 'May'}, {'value': 6, 'name': 'June'},
    {'value': 7, 'name': 'July'}, {'value': 8, 'name': 'August'},
    {'value': 9, 'name': 'September'}, {'value': 10, 'name': 'October'},
    {'value': 11, 'name': 'November'}, {'value': 12, 'name': 'December'}
]

@app.route('/')
def index():
    try:
        # Convert log-normalized prices back to original scale for visualization
        if 'log_normalized_price' in food_df.columns:
            food_df['price'] = food_df['log_normalized_price'].apply(exp)
        else:
            food_df['price'] = food_df['normalized_price']  # Fallback if log column doesn't exist
            
        trend_fig = px.line(food_df.groupby(['year', 'category']).agg({'price': 'mean'}).reset_index(),
                           x='year', y='price', color='category',
                           title='Average Food Price Trends by Category Over Years',
                           labels={'year': 'Year', 'price': 'Price (KES)', 'category': 'Category'})
        trend_graph = json.dumps(trend_fig, cls=plotly.utils.PlotlyJSONEncoder)
        
        latest_year = food_df['year'].max()
        latest_prices = food_df[food_df['year'] == latest_year]
        dist_fig = px.box(latest_prices, x='category', y='price',
                         title=f'Price Distribution Across Food Categories ({latest_year})',
                         labels={'category': 'Category', 'price': 'Price (KES)'})
        dist_graph = json.dumps(dist_fig, cls=plotly.utils.PlotlyJSONEncoder)
        
        merged_data = pd.merge(food_df, rainfall_data, on=['year', 'month'])
        rainfall_fig = px.scatter(merged_data, x='avg_rainfall_mm', y='price', color='category',
                                title='Rainfall vs Food Prices by Category',
                                labels={'avg_rainfall_mm': 'Average Rainfall (mm)', 
                                       'price': 'Price (KES)',
                                       'category': 'Category'})
        rainfall_graph = json.dumps(rainfall_fig, cls=plotly.utils.PlotlyJSONEncoder)
        
        return render_template('index.html', 
                            trend_graph=trend_graph,
                            dist_graph=dist_graph,
                            rainfall_graph=rainfall_graph,
                            model_available=model is not None)
    
    except Exception as e:
        print(f"Error generating visualizations: {str(e)}")
        return render_template('error.html', message="Error loading visualization data")

@app.route('/predict', methods=['GET', 'POST'])
def predict():
    # Ensure Spark and model are loaded
    spark = create_spark_session()
    model = load_model()
    
    if request.method == 'POST':
        if model is None or spark is None:
            return render_template('predict.html', 
                                 regions=regions,
                                 counties=counties,
                                 markets=markets,
                                 categories=categories,
                                 commodities=commodities,
                                 months=months,
                                 prediction="Error: Prediction model or Spark session not loaded",
                                 model_available=False)
        
        try:
            # Get form data
            region = request.form['region']
            county = request.form['county']
            market = request.form['market']
            category = request.form['category']
            commodity = request.form['commodity']
            month = int(request.form['month'])
            year = int(request.form['year'])
            quantity = float(request.form['quantity'])
            
            # Get rainfall data
            try:
                rainfall_row = rainfall_data[(rainfall_data['month'] == month) & 
                                           (rainfall_data['year'] == year)]
                avg_rainfall_mm = rainfall_row['avg_rainfall_mm'].values[0] if not rainfall_row.empty else 100.0
            except Exception as e:
                print(f"Error getting rainfall data: {str(e)}")
                avg_rainfall_mm = 100.0  # Default value
            
            # Create Spark DataFrame for prediction
            schema = StructType([ 
                StructField("region", StringType(), True),
                StructField("county", StringType(), True),
                StructField("market", StringType(), True),
                StructField("category", StringType(), True),
                StructField("commodity", StringType(), True),
                StructField("month", IntegerType(), True),
                StructField("year", IntegerType(), True),
                StructField("avg_rainfall_mm", DoubleType(), True)
            ])
            
            input_data = [(region, county, market, category, commodity, month, year, avg_rainfall_mm)]
            input_df = spark.createDataFrame(input_data, schema)
            
            # Make prediction (returns log-normalized price)
            prediction = model.transform(input_df)
            log_predicted_price = prediction.select("prediction").collect()[0][0]
            
            # Convert back to original price scale
            predicted_price = exp(log_predicted_price)
            total_price = predicted_price * quantity
            
            # Format the output
            return render_template('predict.html', 
                                regions=regions,
                                counties=counties,
                                markets=markets,
                                categories=categories,
                                commodities=commodities,
                                months=months,
                                prediction=f"Predicted Price: KES {predicted_price:,.2f} per unit, Total Price: KES {total_price:,.2f}",
                                model_available=True)
            
        except Exception as e:
            print(f"Error during prediction: {str(e)}")
            return render_template('predict.html', 
                                regions=regions,
                                counties=counties,
                                markets=markets,
                                categories=categories,
                                commodities=commodities,
                                months=months,
                                prediction=f"Error: {str(e)}",
                                model_available=True)
    
    return render_template('predict.html', 
                         regions=regions,
                         counties=counties,
                         markets=markets,
                         categories=categories,
                         commodities=commodities,
                         months=months,
                         prediction=None,
                         model_available=model is not None)

@app.route('/get_counties/<region>')
def get_counties(region):  # Fixed typo in function name
    try:
        filtered_counties = county_data[county_data['region'] == region]['county'].unique()
        return jsonify(sorted(filtered_counties.tolist()))
    except:
        return jsonify([])

@app.route('/get_commodities/<category>')
def get_commodities(category):  # Fixed typo in function name
    try:
        filtered_commodities = food_df[food_df['category'] == category]['commodity'].unique()
        return jsonify(sorted(filtered_commodities.tolist()))
    except:
        return jsonify([])


@app.route('/get_markets/<county>')
def get_markets(county):
    try:
        filtered = county_data[county_data['county'].str.lower() == county.lower()]
        markets = sorted(filtered['market'].dropna().unique())
        return jsonify(markets)
    except Exception as e:
        print(f"Error retrieving markets: {str(e)}")
        return jsonify([]), 500


@app.teardown_appcontext
def shutdown_session(exception=None):
    spark.stop()
    
if __name__ == '__main__':
    # Pre-load model when starting the app
    create_spark_session()
    load_model()
    app.run(debug=True)