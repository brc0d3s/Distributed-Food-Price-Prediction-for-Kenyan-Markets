from flask import Flask, render_template, request, jsonify
from pyspark.ml.regression import GBTRegressionModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession

app = Flask(__name__)

# Dropdown data
dropdown_data = {
    "region": ['Rift Valley', 'Eastern', 'North Eastern', 'Nyanza', 'Coast', 'Central', 'Nairobi'],
    "county": ['Uasin Gishu', 'Nakuru', 'Mandera', 'Kisumu', 'Marsabit', 'Wajir', 'Kajiado', 'Turkana', 'Mombasa', 'Kwale', 'Makueni', 'Meru South', 'Garissa', 'Nairobi', 'Isiolo', 'Kitui', 'Kilifi', 'Baringo', 'West Pokot', 'Nyeri', 'Machakos'],
    "market": ['Kaanwa (Tharaka Nithi)', 'Illbissil Food Market (Kajiado)', 'IFO (Daadab)', 'Kibra (Nairobi)', 'Dadaab town', 'Kalahari (Mombasa)', 'Mathare (Nairobi)', 'Kitengela (Kajiado)', 'Kakuma 3', 'Mukuru (Nairobi)', 'Shonda (Mombasa)', 'Nakuru', 'Lodwar town', 'Moroto (Mombasa)', 'Kalobeyei (Village 3)', 'Wote town (Makueni)', 'Dandora (Nairobi)', 'Kakuma 2', 'Kakuma 4', 'Wakulima (Nakuru)', 'Vanga (Kwale)', 'Takaba (Mandera)', 'Wajir town', 'Dagahaley (Daadab)', 'Mandera', 'Kisumu', 'Lomut (West Pokot)', 'Mogadishu (Kakuma)', 'Ethiopia (Kakuma)', 'HongKong (Kakuma)', 'Marsabit', 'Bangladesh (Mombasa)', 'Garissa town (Garissa)', 'Kajiado', 'Mombasa', 'Kitui town (Kitui)', 'Kathonzweni (Makueni)', 'Isiolo town', 'Tala Centre Market (Machakos)', 'Kangemi (Nairobi)', 'Lodwar (Turkana)', 'Eldoret town (Uasin Gishu)', 'Makutano (West Pokot)', 'Kawangware (Nairobi)', 'Kongowea (Mombasa)', 'Makueni', 'Garissa', 'Nairobi', 'Marsabit town', 'Wakulima (Nairobi)', 'Kalobeyei (Village 1)', 'Kitui', 'Junda (Mombasa)', 'Kilifi', 'Karatina (Nyeri)', 'Marigat (Baringo)', 'Hagadera (Daadab)', 'Kalobeyei (Village 2)', 'Kisumu Ndogo (Mombasa)', 'Kibuye (Kisumu)', 'Marigat town (Baringo)'],
    "category": ['milk and dairy', 'pulses and nuts', 'non-food', 'meat, fish and eggs', 'vegetables and fruits', 'oil and fats', 'cereals and tubers', 'miscellaneous food'],
    "commodity": ['Maize flour', 'Meat (camel)', 'Beans (mung)', 'Milk (cow, pasteurized)', 'Salt', 'Maize (white)', 'Wheat flour', 'Cowpea leaves', 'Sorghum', 'Beans (kidney)', 'Beans', 'Rice (imported, Pakistan)', 'Milk (camel, fresh)', 'Maize', 'Potatoes (Irish, white)', 'Tomatoes', 'Oil (vegetable)', 'Meat (beef)', 'Cabbage', 'Beans (dolichos)', 'Fuel (petrol-gasoline)', 'Cowpeas', 'Beans (yellow)', 'Cooking fat', 'Sorghum (white)', 'Beans (dry)', 'Spinach', 'Millet (finger)', 'Fish (omena, dry)', 'Potatoes (Irish)', 'Rice', 'Rice (aromatic)', 'Bread', 'Potatoes (Irish, red)', 'Fuel (diesel)', 'Meat (goat)', 'Sorghum (red)', 'Onions (dry)', 'Kale', 'Milk (cow, fresh)', 'Onions (red)', 'Sugar', 'Milk (UHT)', 'Maize (white, dry)', 'Bananas', 'Fuel (kerosene)', 'Beans (rosecoco)'],
    "unit": ['Bunch', '400 G', '64 KG', 'Head', 'L', '200 G', '50 KG', '13 KG', '90 KG', '126 KG', 'KG', 'Unit', '500 ML']
}

@app.route('/dropdown-data', methods=['GET'])
def get_dropdown_data():
    return jsonify(dropdown_data)

# Initialize Spark
spark = SparkSession.builder.appName("FoodPricePrediction").getOrCreate()

# Load the trained pipeline model
model_path = "models/gbt_price_prediction_model"  # Ensure this path is correct
model = PipelineModel.load(model_path)

feature_cols = ['region_index', 'county_index', 'market_index', 'category_index', 'commodity_index', 'unit_index', 'latitude', 'longitude']

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/predict', methods=['POST'])
def predict():
    try:
        data = request.json

        input_data = spark.createDataFrame([
            (data['region_index'], data['county_index'], data['market_index'],
             data['category_index'], data['commodity_index'], data['unit_index'],
             float(data['latitude']), float(data['longitude']))
        ], feature_cols)

        assembler = VectorAssembler(inputCols=feature_cols, outputCol='features')
        assembled_data = assembler.transform(input_data)

        prediction = model.transform(assembled_data).select('prediction').collect()[0]['prediction']

        return jsonify({'predicted_price': prediction})
    except Exception as e:
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    app.run(debug=True)
