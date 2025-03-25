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
        });

    function populateDropdown(id, options) {
        let select = document.getElementById(id);
        options.forEach(option => {
            let opt = document.createElement("option");
            opt.value = option;
            opt.innerHTML = option;
            select.appendChild(opt);
        });
    }
});


function predictPrice() {
    let data = {
        region_index: document.getElementById("region").selectedIndex,
        county_index: document.getElementById("county").selectedIndex,
        market_index: document.getElementById("market").selectedIndex,
        category_index: document.getElementById("category").selectedIndex,
        commodity_index: document.getElementById("commodity").selectedIndex,
        unit_index: document.getElementById("unit").selectedIndex,
        latitude: document.getElementById("latitude").value,
        longitude: document.getElementById("longitude").value
    };

    fetch('/predict', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data)
    })
    .then(response => response.json())
    .then(result => {
        document.getElementById("predictionResult").innerText = "Predicted Price: " + result.predicted_price;
    })
    .catch(error => console.error('Error:', error));
}
