document.addEventListener('DOMContentLoaded', function () {
    const regionSelect = document.getElementById('region');
    const countySelect = document.getElementById('county');
    const marketSelect = document.getElementById('market');
    const categorySelect = document.getElementById('category');
    const commoditySelect = document.getElementById('commodity');

    // Region -> County
    if (regionSelect && countySelect) {
        regionSelect.addEventListener('change', function () {
            const region = this.value;
            countySelect.disabled = !region;
            countySelect.innerHTML = '<option value="" selected disabled>Loading counties...</option>';
            marketSelect.disabled = true;
            marketSelect.innerHTML = '<option value="" selected disabled>Select Market</option>';

            if (region) {
                fetch(`/get_counties/${encodeURIComponent(region)}`)
                    .then(response => {
                        if (!response.ok) throw new Error('Network response was not ok');
                        return response.json();
                    })
                    .then(data => {
                        countySelect.innerHTML = '<option value="" selected disabled>Select County</option>';
                        data.forEach(county => {
                            const option = document.createElement('option');
                            option.value = county;
                            option.textContent = county;
                            countySelect.appendChild(option);
                        });
                    })
                    .catch(error => {
                        console.error('Error loading counties:', error);
                        countySelect.innerHTML = '<option value="" selected disabled>Error loading counties</option>';
                    });
            }
        });

        // County -> Market
countySelect.addEventListener('change', function () {
    const county = this.value;
    marketSelect.disabled = !county;
    marketSelect.innerHTML = '<option value="" selected disabled>Loading markets...</option>';

    if (county) {
        fetch(`/get_markets/${encodeURIComponent(county)}`)
            .then(response => {
                if (!response.ok) throw new Error('Network response was not ok');
                return response.json();
            })
            .then(data => {
                marketSelect.innerHTML = '<option value="" selected disabled>Select Market</option>';
                data.forEach(market => {
                    const option = document.createElement('option');
                    option.value = market;
                    option.textContent = market;
                    marketSelect.appendChild(option);
                });
            })
            .catch(error => {
                console.error('Error loading markets:', error);
                marketSelect.innerHTML = '<option value="" selected disabled>Error loading markets</option>';
            });
    }
});
    }

    // Category -> Commodity
    if (categorySelect && commoditySelect) {
        categorySelect.addEventListener('change', function () {
            const category = this.value;
            commoditySelect.disabled = !category;
            commoditySelect.innerHTML = '<option value="" selected disabled>Loading commodities...</option>';

            if (category) {
                fetch(`/get_commodities/${encodeURIComponent(category)}`)
                    .then(response => {
                        if (!response.ok) throw new Error('Network response was not ok');
                        return response.json();
                    })
                    .then(data => {
                        commoditySelect.innerHTML = '<option value="" selected disabled>Select Commodity</option>';
                        data.forEach(commodity => {
                            const option = document.createElement('option');
                            option.value = commodity;
                            option.textContent = commodity;
                            commoditySelect.appendChild(option);
                        });
                    })
                    .catch(error => {
                        console.error('Error loading commodities:', error);
                        commoditySelect.innerHTML = '<option value="" selected disabled>Error loading commodities</option>';
                    });
            }
        });
    }
});