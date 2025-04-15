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

# Initialize Spark and model
spark = None
model = None

try:
    # Initialize Spark session
    spark = SparkSession.builder.appName("FoodPriceApp").getOrCreate()
    
    # Load model with path verification
    model_path = "../model/log_price_pipeline_model"
    if os.path.exists(model_path):
        model = PipelineModel.load(model_path)
        print("Model loaded successfully")
    else:
        print(f"Error: Model not found at {os.path.abspath(model_path)}")
except Exception as e:
    print(f"Error initializing Spark or loading model: {str(e)}")
    if spark:
        spark.stop()

# Load data with error handling
try:
    # County, Region, market mapping
    county_data = pd.read_csv("../data/county_region.csv")
    
    # Main food price data
    food_df = pd.read_csv("../data/commodity_category_price.csv")
    
    # Rainfall data
    rainfall_data = pd.read_csv("../data/monthly_rainfall.csv")
    
    # Prepare dropdown options - ensure column names match your CSV exactly
    regions = sorted(county_data['region'].dropna().unique())
    counties = sorted(county_data['county'].dropna().unique())
    markets = sorted(county_data['market'].dropna().unique())
    categories = sorted(food_df['category'].dropna().unique())
    commodities = sorted(food_df['commodity'].dropna().unique())
    
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
        # Ensure column names match your actual data
        trend_fig = px.line(food_df.groupby(['year', 'category']).agg({'normalized_price': 'mean'}).reset_index(),
                           x='year', y='normalized_price', color='category',
                           title='Average Food Price Trends by Category Over Years',
                           labels={'year': 'Year', 'normalized_price': 'Normalized Price', 'category': 'Category'})
        trend_graph = json.dumps(trend_fig, cls=plotly.utils.PlotlyJSONEncoder)
        
        latest_year = food_df['year'].max()
        latest_prices = food_df[food_df['year'] == latest_year]
        dist_fig = px.box(latest_prices, x='category', y='normalized_price',
                         title=f'Price Distribution Across Food Categories ({latest_year})',
                         labels={'category': 'Category', 'normalized_price': 'Normalized Price'})
        dist_graph = json.dumps(dist_fig, cls=plotly.utils.PlotlyJSONEncoder)
        
        merged_data = pd.merge(food_df, rainfall_data, on=['year', 'month'])
        rainfall_fig = px.scatter(merged_data, x='avg_rainfall_mm', y='normalized_price', color='category',
                                title='Rainfall vs Food Prices by Category',
                                labels={'avg_rainfall_mm': 'Average Rainfall (mm)', 
                                       'normalized_price': 'Normalized Price',
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
    if request.method == 'POST':
        if model is None:
            return render_template('predict.html', 
                                 regions=regions,
                                 counties=counties,
                                 markets=markets,
                                 categories=categories,
                                 commodities=commodities,
                                 months=months,
                                 prediction="Error: Prediction model not loaded",
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
            
            # Create Spark DataFrame for prediction
            schema = StructType([
                StructField("region", StringType(), True),
                StructField("county", StringType(), True),
                StructField("market", StringType(), True),
                StructField("category", StringType(), True),
                StructField("commodity", StringType(), True),
                StructField("month", IntegerType(), True),
                StructField("year", IntegerType(), True),
                StructField("quantity", DoubleType(), True)
            ])
            
            input_data = [(region, county, market, category, commodity, month, year, quantity)]
            input_df = spark.createDataFrame(input_data, schema)
            
            # Make prediction
            prediction = model.transform(input_df)
            predicted_price = prediction.select("prediction").collect()[0][0]
            final_price = round(float(predicted_price), 2)
            
            return render_template('predict.html', 
                                regions=regions,
                                counties=counties,
                                markets=markets,
                                categories=categories,
                                commodities=commodities,
                                months=months,
                                prediction=f"Predicted Price: KES {final_price:,.2f}",
                                model_available=True)
            
        except Exception as e:
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
def get_counties(region):
    try:
        filtered_counties = county_data[county_data['region'].str.lower() == region.lower()]['county'].unique()
        return jsonify(sorted(filtered_counties.tolist()))
    except Exception as e:
        print(f"Error getting counties: {str(e)}")
        return jsonify([])

@app.route('/get_markets/<county>')
def get_markets(county):
    try:
        filtered_markets = county_data[county_data['county'].str.lower() == county.lower()]['market'].unique()
        return jsonify(sorted(filtered_markets.tolist()))
    except Exception as e:
        print(f"Error getting markets: {str(e)}")
        return jsonify([])

@app.route('/get_commodities/<category>')
def get_commodities(category):
    try:
        filtered_commodities = food_df[food_df['category'].str.lower() == category.lower()]['commodity'].unique()
        return jsonify(sorted(filtered_commodities.tolist()))
    except Exception as e:
        print(f"Error getting commodities: {str(e)}")
        return jsonify([])

@app.teardown_appcontext
def shutdown_session(exception=None):
    if spark:
        spark.stop()

if __name__ == '__main__':
    app.run(debug=True)