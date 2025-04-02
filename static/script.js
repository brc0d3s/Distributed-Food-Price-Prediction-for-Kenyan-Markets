document.addEventListener("DOMContentLoaded", function () {
    fetch('/dropdown-data')
        .then(response => response.json())
        .then(data => {
            populateDropdown('region', data.region);
            populateDropdown('county', data.county);
            populateDropdown('market', data.market);
            populateDropdown('category', data.category);
            populateDropdown('commodity', data.commodity);
            populateDropdown('unit', data.unit);
        })
        .catch(error => console.error('Error fetching dropdown data:', error));

    function populateDropdown(id, options) {
        let select = document.getElementById(id);
        select.innerHTML = '<option value="" disabled selected>Select an option</option>';
        options.forEach((option) => {
            let opt = document.createElement("option");
            opt.value = option; 
            opt.innerText = option;
            select.appendChild(opt);
        });
    }
});

function predictPrice() {
    if (!validateSelection()) {
        document.getElementById("predictionResult").innerText = "Please fill all fields!";
        return;
    }

    let data = {
        region: document.getElementById("region").value,
        county: document.getElementById("county").value,
        market: document.getElementById("market").value,
        category: document.getElementById("category").value,
        commodity: document.getElementById("commodity").value,
        unit: document.getElementById("unit").value,
        latitude: -4.05, 
        longitude: 39.666667
    };

    if (navigator.geolocation) {
        navigator.geolocation.getCurrentPosition(
            (position) => {
                data.latitude = position.coords.latitude;
                data.longitude = position.coords.longitude;
                sendPredictionRequest(data);
            },
            () => {
                console.warn("Geolocation permission denied. Using default (-4.05, 39.666667).");
                sendPredictionRequest(data);
            }
        );
    } else {
        sendPredictionRequest(data);
    }
}

function sendPredictionRequest(data) {
    fetch('/predict', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data)
    })
    .then(response => response.json())
    .then(result => {
        if (result.predicted_price) {
            let final_price = (result.predicted_price * 130).toFixed(2);
            document.getElementById("predictionResult").innerText = `Predicted Price: ${final_price}`;
        } else {
            document.getElementById("predictionResult").innerText = `Error: ${result.error}`;
        }
    })
    .catch(error => {
        document.getElementById("predictionResult").innerText = "Error: Unable to fetch prediction.";
        console.error('Error:', error);
    });
}


function validateSelection() {
    return ["region", "county", "market", "category", "commodity", "unit"].every(id => {
        let value = document.getElementById(id).value;
        return value !== "" && value !== null;
    });
}
