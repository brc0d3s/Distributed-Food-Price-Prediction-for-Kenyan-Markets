document.addEventListener('DOMContentLoaded', function() {
    // County dropdown population based on region selection
    const regionSelect = document.getElementById('region');
    const countySelect = document.getElementById('county');
    const marketSelect = document.getElementById('market');
    const categorySelect = document.getElementById('category');
    const commoditySelect = document.getElementById('commodity');
    
    // Region -> County -> Market chain
    if (regionSelect && countySelect && marketSelect) {
        regionSelect.addEventListener('change', function() {
            const region = this.value;
            countySelect.disabled = !region;
            marketSelect.disabled = true;
            
            if (region) {
                fetch(`/get_counties/${encodeURIComponent(region)}`)
                    .then(response => response.json())
                    .then(data => {
                        countySelect.innerHTML = '<option value="" selected disabled>Select County</option>';
                        marketSelect.innerHTML = '<option value="" selected disabled>Select Market</option>';
                        data.forEach(county => {
                            const option = document.createElement('option');
                            option.value = county;
                            option.textContent = county;
                            countySelect.appendChild(option);
                        });
                    })
                    .catch(error => console.error('Error loading counties:', error));
            }
        });
        
        countySelect.addEventListener('change', function() {
            const county = this.value;
            marketSelect.disabled = !county;
            
            if (county) {
                fetch(`/get_markets/${encodeURIComponent(county)}`)
                    .then(response => response.json())
                    .then(data => {
                        marketSelect.innerHTML = '<option value="" selected disabled>Select Market</option>';
                        data.forEach(market => {
                            const option = document.createElement('option');
                            option.value = market;
                            option.textContent = market;
                            marketSelect.appendChild(option);
                        });
                    })
                    .catch(error => console.error('Error loading markets:', error));
            }
        });
    }
    
    // Category -> Commodity chain
    if (categorySelect && commoditySelect) {
        categorySelect.addEventListener('change', function() {
            const category = this.value;
            commoditySelect.disabled = !category;
            
            if (category) {
                fetch(`/get_commodities/${encodeURIComponent(category)}`)
                    .then(response => response.json())
                    .then(data => {
                        commoditySelect.innerHTML = '<option value="" selected disabled>Select Commodity</option>';
                        data.forEach(commodity => {
                            const option = document.createElement('option');
                            option.value = commodity;
                            option.textContent = commodity;
                            commoditySelect.appendChild(option);
                        });
                    })
                    .catch(error => console.error('Error loading commodities:', error));
            }
        });
    }
});