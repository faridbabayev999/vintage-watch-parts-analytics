/**
 * Vintage Watch Spare Parts — Valuation Intelligence Dashboard
 * Client-Side SPA Controller & Data Binder
 */

// Global Application State
const state = {
    currentRoute: '#portfolio',
    portfolioData: null,
    itemsData: null, // Cache for priced and unpriced items
    selectedItem: null, // Currently viewed item details
    filters: {
        search: '',
        brand: 'all',
        confidence: 'all',
        turnover: 'all',
        availability: 'all'
    },
    sort: {
        column: 'name',
        ascending: true
    }
};

// REST API Base URL (Relative to server)
const API_BASE = '/api';

// Initialize App
document.addEventListener('DOMContentLoaded', () => {
    initRouter();
    initEventListeners();
    fetchFreshness();
    fetchPortfolio();
    fetchItems();
});

// --- SPA Routing ---
function initRouter() {
    // Route handler based on hash
    const handleRoute = () => {
        const hash = window.location.hash || '#portfolio';
        state.currentRoute = hash;

        // Parse query params if present (e.g. #item?id=rolex_32_557b)
        let route = hash;
        let queryParams = {};

        if (hash.includes('?')) {
            const parts = hash.split('?');
            route = parts[0];
            const queryStr = parts[1];
            queryParams = parseQueryString(queryStr);
        }

        // Toggle active navigation
        document.querySelectorAll('.nav-item').forEach(item => {
            if (item.getAttribute('href') === route) {
                item.classList.add('active');
            } else {
                item.classList.remove('active');
            }
        });

        // Toggle page sections
        if (route === '#portfolio') {
            showSection('portfolio-page');
            renderPortfolio();
        } else if (route === '#inventory') {
            showSection('inventory-page');
            renderInventoryTable();
        } else if (route === '#item') {
            showSection('detail-page');
            if (queryParams.id) {
                loadItemDetail(queryParams.id);
            } else {
                window.location.hash = '#inventory';
            }
        } else if (route === '#add-item') {
            showSection('add-item-page');
            initAddItemPage();
        } else {
            // Default fallback
            window.location.hash = '#portfolio';
        }
    };

    window.addEventListener('hashchange', handleRoute);
    // Execute on initial page load
    handleRoute();
}

function showSection(sectionId) {
    const sections = ['portfolio-page', 'inventory-page', 'detail-page', 'add-item-page'];
    sections.forEach(id => {
        const el = document.getElementById(id);
        if (id === sectionId) {
            el.classList.remove('hidden');
        } else {
            el.classList.add('hidden');
        }
    });
}

function parseQueryString(str) {
    const params = {};
    const pairs = str.split('&');
    for (const pair of pairs) {
        const keyVal = pair.split('=');
        if (keyVal.length === 2) {
            params[decodeURIComponent(keyVal[0])] = decodeURIComponent(keyVal[1]);
        }
    }
    return params;
}

// --- Event Listeners ---
function initEventListeners() {
    // Header navigation clicks
    document.getElementById('nav-portfolio').addEventListener('click', (e) => {
        e.preventDefault();
        window.location.hash = '#portfolio';
    });

    document.getElementById('nav-inventory').addEventListener('click', (e) => {
        e.preventDefault();
        window.location.hash = '#inventory';
    });

    document.getElementById('nav-add-item').addEventListener('click', (e) => {
        e.preventDefault();
        window.location.hash = '#add-item';
    });

    // Back to inventory button
    document.getElementById('btn-back-to-inventory').addEventListener('click', (e) => {
        e.preventDefault();
        window.location.hash = '#inventory';
    });

    // Filters event listeners
    document.getElementById('inventory-search').addEventListener('input', (e) => {
        state.filters.search = e.target.value;
        renderInventoryTable();
    });

    document.getElementById('filter-brand').addEventListener('change', (e) => {
        state.filters.brand = e.target.value;
        renderInventoryTable();
    });

    document.getElementById('filter-confidence').addEventListener('change', (e) => {
        state.filters.confidence = e.target.value;
        renderInventoryTable();
    });

    document.getElementById('filter-turnover').addEventListener('change', (e) => {
        state.filters.turnover = e.target.value;
        renderInventoryTable();
    });

    document.getElementById('filter-availability').addEventListener('change', (e) => {
        state.filters.availability = e.target.value;
        renderInventoryTable();
    });

    document.getElementById('btn-download-csv').addEventListener('click', () => {
        downloadInventoryCSV();
    });

    // Unpriced items section toggle
    const toggleBtn = document.getElementById('unpriced-toggle-btn');
    const panel = document.getElementById('unpriced-panel');
    toggleBtn.addEventListener('click', () => {
        toggleBtn.classList.toggle('active');
        panel.classList.toggle('open');
    });

    // Price Simulator Slider
    const slider = document.getElementById('price-simulator-slider');
    const percentageVal = document.getElementById('slider-percentage-val');
    const resetBtn = document.getElementById('btn-reset-simulator');

    const updateSimulation = () => {
        if (!state.selectedItem) return;
        const pct = parseInt(slider.value);
        percentageVal.textContent = pct === 0 ? 'Suggested Price (0%)' : `Adjusted Price (${pct > 0 ? '+' : ''}${pct}%)`;
        calculateClientSimulation(pct);
    };

    slider.addEventListener('input', updateSimulation);
    
    resetBtn.addEventListener('click', () => {
        slider.value = 0;
        updateSimulation();
    });

    // Table sorting headers
    document.querySelectorAll('.sortable-header').forEach(header => {
        header.addEventListener('click', () => {
            const col = header.getAttribute('data-sort');
            if (state.sort.column === col) {
                state.sort.ascending = !state.sort.ascending;
            } else {
                state.sort.column = col;
                state.sort.ascending = true;
            }
            updateSortIcons();
            renderInventoryTable();
        });
    });
}

// --- Fetch Data ---
function fetchFreshness() {
    fetch(`${API_BASE}/portfolio`)
        .then(res => res.json())
        .then(data => {
            const freshnessEl = document.getElementById('header-freshness');
            if (freshnessEl) {
                if (data.freshness && data.freshness.tmv_computed_at !== '—') {
                    const dateStr = data.freshness.tmv_computed_at.split('.')[0];
                    freshnessEl.textContent = `Priced: ${dateStr}`;
                } else {
                    freshnessEl.textContent = 'Data Standby';
                }
            }
        })
        .catch(err => {
            console.error('Error fetching freshness:', err);
            const freshnessEl = document.getElementById('header-freshness');
            if (freshnessEl) freshnessEl.textContent = 'Server Offline';
        });
}

function fetchPortfolio() {
    fetch(`${API_BASE}/portfolio`)
        .then(res => res.json())
        .then(data => {
            state.portfolioData = data;
            renderPortfolio();
        })
        .catch(err => {
            console.error('Error fetching portfolio statistics:', err);
        });
}

function fetchItems() {
    fetch(`${API_BASE}/items`)
        .then(res => res.json())
        .then(data => {
            state.itemsData = data;
            updateSortIcons();
            renderInventoryTable();
            renderUnpricedItems();
        })
        .catch(err => {
            console.error('Error fetching items data:', err);
            const tbody = document.getElementById('items-table-body');
            tbody.innerHTML = `<tr><td colspan="6" class="table-loading" style="color:var(--headlight-red-text)">Failed to load data from server. Ensure Python server is running.</td></tr>`;
        });
}

function loadItemDetail(itemId) {
    // Set UI to loading state
    document.getElementById('detail-title').textContent = 'Loading item details...';
    
    fetch(`${API_BASE}/item?id=${encodeURIComponent(itemId)}`)
        .then(res => res.json())
        .then(data => {
            if (data.error) {
                document.getElementById('detail-title').textContent = 'Error Loading Item';
                alert(data.error);
                return;
            }
            state.selectedItem = data;
            renderItemDetail();
        })
        .catch(err => {
            console.error('Error loading item detail:', err);
            document.getElementById('detail-title').textContent = 'Connection Error';
        });
}

function pollPipelineJob(jobId, statusElement, canonicalId = null, attempts = 0) {
    if (!jobId || !statusElement) return;
    fetch(`${API_BASE}/job?id=${encodeURIComponent(jobId)}`)
        .then(res => res.json())
        .then(data => {
            if (data.error) {
                statusElement.innerHTML = `<div class="status-error-card">${escapeHtml(data.error)}</div>`;
                return;
            }
            const job = data.job || {};
            const contract = data.contract || {};
            const status = job.status || 'UNKNOWN';
            const latestEvent = data.events && data.events.length ? data.events[0].message : 'Pipeline queued.';

            if (status === 'SUCCEEDED') {
                const priceText = contract && contract.recommended_price_eur !== null && contract.recommended_price_eur !== undefined
                    ? formatCurrency(contract.recommended_price_eur)
                    : 'No recommendation yet';
                const sellText = contract && contract.sell_time_display ? contract.sell_time_display : 'Sell-time unavailable';
                statusElement.innerHTML = `
                    <div class="status-success-card">
                        <strong>Pipeline complete.</strong>
                        <p>Price: ${priceText}. Sell time: ${escapeHtml(sellText)}.</p>
                    </div>
                `;
                fetchItems();
                fetchPortfolio();
                if (canonicalId) {
                    loadItemDetail(canonicalId);
                }
                return;
            }

            if (status === 'FAILED') {
                statusElement.innerHTML = `
                    <div class="status-error-card">
                        <strong>Pipeline failed.</strong>
                        <p>${escapeHtml(job.error_message || latestEvent || 'Check logs for details.')}</p>
                    </div>
                `;
                return;
            }

            statusElement.innerHTML = `
                <div class="status-info-card">
                    <strong>Pipeline ${escapeHtml(status.toLowerCase())}...</strong>
                    <p>${escapeHtml(latestEvent || '')}</p>
                </div>
            `;
            if (attempts < 120) {
                setTimeout(() => pollPipelineJob(jobId, statusElement, canonicalId, attempts + 1), 2000);
            }
        })
        .catch(err => {
            console.error("Error polling pipeline job:", err);
            if (attempts < 10) {
                setTimeout(() => pollPipelineJob(jobId, statusElement, canonicalId, attempts + 1), 3000);
            }
        });
}

// --- Render Portfolio Homepage ---
function renderPortfolio() {
    if (!state.portfolioData) return;

    const { overview, price_distribution, sell_time_distribution, top_brands, top_calibers, market_summary, simulation } = state.portfolioData;

    // 1. KPI Metrics
    const totalPhysical = overview.total_physical_stock || 0;
    const totalUnique = overview.total_inventory || 0;
    const pricedUniqueN = overview.priced_n || 0;
    const pricedUniquePct = overview.priced_pct || 0;
    const portfolioVal = overview.total_portfolio_value_eur || overview.portfolio_value_eur;
    const typicalSellTime = overview.typical_sell_time || '—';
    const highDemandN = overview.high_demand_n || 0;

    // Render KPI values in HTML elements
    document.getElementById('kpi-total-unique-parts').textContent = totalUnique.toLocaleString();
    document.getElementById('kpi-priced-n').textContent = pricedUniqueN.toLocaleString();
    document.getElementById('kpi-priced-pct-subtext').textContent = `${pricedUniquePct.toFixed(1)}% of inventory`;
    document.getElementById('kpi-portfolio-val').textContent = portfolioVal ? formatCurrency(portfolioVal) : '—';
    document.getElementById('kpi-total-physical-stock').textContent = totalPhysical.toLocaleString();
    document.getElementById('kpi-typical-sell-time').textContent = typicalSellTime;
    document.getElementById('kpi-high-demand-n').textContent = highDemandN.toLocaleString();

    // 2. Render portfolio turnover timeline from granular turnover buckets
    if (simulation) {
        const buckets = simulation.bucket_totals || [];
        const forecastedUnits = buckets.reduce((sum, bucket) => sum + Number(bucket.units || 0), 0);
        const forecastRevenue = buckets.reduce((sum, bucket) => sum + Number(bucket.revenue || 0), 0);
        const remainingUnits = Number(simulation.units_remaining || 0);
        const timelineTotal = totalPhysical || (forecastedUnits + remainingUnits) || 1;
        const soldThroughPct = Math.min((forecastedUnits / timelineTotal) * 100, 100);
        const displayedForecastedUnits = Math.round(forecastedUnits);
        const displayedRemainingUnits = Math.round(remainingUnits);
        const displayedBucketUnits = allocateWholeUnits(
            buckets.map(bucket => Number(bucket.units || 0)),
            displayedForecastedUnits
        );

        const headline = document.getElementById('portfolio-turnover-headline');
        const subline = document.getElementById('portfolio-turnover-subline');
        const revenue = document.getElementById('portfolio-turnover-revenue');
        const track = document.getElementById('portfolio-turnover-track');
        const bucketList = document.getElementById('portfolio-turnover-buckets');

        if (headline) {
            headline.textContent = `${formatUnitCount(displayedForecastedUnits)} of ${formatUnitCount(timelineTotal)} units forecasted to sell`;
        }
        if (subline) {
            subline.textContent = `${soldThroughPct.toFixed(1)}% expected sell-through; ${formatUnitCount(displayedRemainingUnits)} units remain outside the current forecast horizon`;
        }
        if (revenue) {
            revenue.textContent = formatCurrency(forecastRevenue);
        }
        if (track) {
            const bucketSegments = buckets.map((bucket, index) => {
                const width = Math.max((Number(bucket.units || 0) / timelineTotal) * 100, 0);
                return `
                    <div class="turnover-segment segment-${index + 1}" style="width: ${width}%">
                        <span>${escapeHtml(bucket.label)}</span>
                    </div>
                `;
            }).join('');
            const remainingWidth = Math.max((remainingUnits / timelineTotal) * 100, 0);
            track.innerHTML = `
                ${bucketSegments}
                ${remainingUnits > 0 ? `
                    <div class="turnover-segment segment-remaining" style="width: ${remainingWidth}%">
                        <span>Remaining</span>
                    </div>
                ` : ''}
            `;
        }
        if (bucketList) {
            let cumulativeUnits = 0;
            bucketList.innerHTML = buckets.map((bucket, index) => {
                const units = Number(bucket.units || 0);
                const displayUnits = displayedBucketUnits[index] || 0;
                cumulativeUnits += units;
                const cumulativePct = timelineTotal ? (cumulativeUnits / timelineTotal) * 100 : 0;
                return `
                    <div class="turnover-bucket-card">
                        <span>${escapeHtml(bucket.label)}</span>
                        <strong>${formatUnitCount(displayUnits)} units</strong>
                        <b>${formatCurrency(bucket.revenue || 0)}</b>
                        <small>${cumulativePct.toFixed(1)}% cumulative</small>
                    </div>
                `;
            }).join('') + `
                <div class="turnover-bucket-card remaining-card">
                    <span>Remaining / Not Forecasted</span>
                    <strong>${formatUnitCount(displayedRemainingUnits)} units</strong>
                    <b>No precise sell-out date</b>
                    <small>${((remainingUnits / timelineTotal) * 100).toFixed(1)}% of stock</small>
                </div>
            `;
        }
    }

    // 3. Chart.js Initializations & Updates
    if (!state.charts) {
        state.charts = {};
    }

    // Chart 1: Inventory Value by Brand (Donut)
    const sortedBrands = [...top_brands].sort((a, b) => b.value_eur - a.value_eur);
    const topN = sortedBrands.slice(0, 3);
    const others = sortedBrands.slice(3);
    
    const brandLabels = topN.map(b => b.brand);
    const brandValues = topN.map(b => b.value_eur);
    const totalBrandVal = top_brands.reduce((sum, b) => sum + b.value_eur, 0) || 1;
    
    if (others.length > 0) {
        const othersVal = others.reduce((sum, b) => sum + b.value_eur, 0);
        brandLabels.push("Others");
        brandValues.push(othersVal);
    }

    if (state.charts.brand) state.charts.brand.destroy();
    const ctxBrand = document.getElementById('brand-donut-chart').getContext('2d');
    state.charts.brand = new Chart(ctxBrand, {
        type: 'doughnut',
        data: {
            labels: brandLabels,
            datasets: [{
                data: brandValues,
                backgroundColor: ['#81B29A', '#A89FBB', '#F4A261', '#D3D3D3'],
                borderWidth: 1.5,
                borderColor: '#FFFFFF'
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'right',
                    labels: {
                        boxWidth: 8,
                        padding: 10,
                        usePointStyle: true,
                        pointStyle: 'circle',
                        font: {
                            family: 'Inter',
                            size: 10
                        },
                        generateLabels: function(chart) {
                            const data = chart.data;
                            if (data.labels.length && data.datasets.length) {
                                return data.labels.map((label, i) => {
                                    const val = data.datasets[0].data[i];
                                    const pct = ((val / totalBrandVal) * 100).toFixed(1);
                                    return {
                                        text: `${label}   ${formatCurrency(val)} (${pct}%)`,
                                        fillStyle: data.datasets[0].backgroundColor[i],
                                        strokeStyle: data.datasets[0].borderColor[i],
                                        lineWidth: data.datasets[0].borderWidth,
                                        hidden: isNaN(data.datasets[0].data[i]) || chart.getDatasetMeta(0).data[i].hidden,
                                        index: i
                                    };
                                });
                            }
                            return [];
                        }
                    }
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            const val = context.raw;
                            const pct = ((val / totalBrandVal) * 100).toFixed(1);
                            return ` ${context.label}: ${formatCurrency(val)} (${pct}%)`;
                        }
                    }
                }
            },
            cutout: '70%'
        }
    });

    // Chart 2: Price Distribution (Bar)
    const priceLabels = price_distribution.map(p => p.bucket);
    const priceCounts = price_distribution.map(p => p.count);

    if (state.charts.price) state.charts.price.destroy();
    const ctxPrice = document.getElementById('price-bar-chart').getContext('2d');
    state.charts.price = new Chart(ctxPrice, {
        type: 'bar',
        data: {
            labels: priceLabels,
            datasets: [{
                label: 'Part Types',
                data: priceCounts,
                backgroundColor: '#81B29A',
                borderRadius: 4,
                barPercentage: 0.6
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            const bin = price_distribution[context.dataIndex];
                            return ` ${context.raw} part types (Val: ${formatCurrency(bin.value_eur)})`;
                        }
                    }
                }
            },
            scales: {
                x: {
                    grid: {
                        display: false
                    },
                    ticks: {
                        font: {
                            family: 'Inter',
                            size: 10
                        }
                    }
                },
                y: {
                    beginAtZero: true,
                    grid: {
                        color: 'rgba(0,0,0,0.05)'
                    },
                    ticks: {
                        font: {
                            family: 'Inter',
                            size: 10
                        }
                    }
                }
            }
        }
    });

    // Chart 3: Sell Time Distribution (Donut)
    const sellLabels = sell_time_distribution.map(s => s.bucket);
    const sellCounts = sell_time_distribution.map(s => s.count);
    const totalSellParts = sell_time_distribution.reduce((sum, s) => sum + s.count, 0) || 1;

    if (state.charts.selltime) state.charts.selltime.destroy();
    const ctxSell = document.getElementById('selltime-donut-chart').getContext('2d');
    state.charts.selltime = new Chart(ctxSell, {
        type: 'doughnut',
        data: {
            labels: sellLabels,
            datasets: [{
                data: sellCounts,
                backgroundColor: ['#81B29A', '#A89FBB', '#F4A261', '#E4AF8E', '#D3D3D3'],
                borderWidth: 1.5,
                borderColor: '#FFFFFF'
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'right',
                    labels: {
                        boxWidth: 8,
                        padding: 10,
                        usePointStyle: true,
                        pointStyle: 'circle',
                        font: {
                            family: 'Inter',
                            size: 10
                        },
                        generateLabels: function(chart) {
                            const data = chart.data;
                            if (data.labels.length && data.datasets.length) {
                                return data.labels.map((label, i) => {
                                    const val = data.datasets[0].data[i];
                                    const pct = ((val / totalSellParts) * 100).toFixed(0);
                                    return {
                                        text: `${label}   ${pct}%`,
                                        fillStyle: data.datasets[0].backgroundColor[i],
                                        strokeStyle: data.datasets[0].borderColor[i],
                                        lineWidth: data.datasets[0].borderWidth,
                                        hidden: isNaN(data.datasets[0].data[i]) || chart.getDatasetMeta(0).data[i].hidden,
                                        index: i
                                    };
                                });
                            }
                            return [];
                        }
                    }
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            const val = context.raw;
                            const pct = ((val / totalSellParts) * 100).toFixed(1);
                            return ` ${context.label}: ${val} parts (${pct}%)`;
                        }
                    }
                }
            },
            cutout: '70%'
        }
    });

    // Chart 4: Top Calibers by Value (Table)
    const caliberBody = document.querySelector('#calibers-table tbody');
    if (caliberBody) {
        caliberBody.innerHTML = '';
        if (top_calibers && top_calibers.length > 0) {
            top_calibers.forEach(c => {
                const tr = document.createElement('tr');
                tr.innerHTML = `
                    <td style="padding: 10px 4px; font-weight: 500;">${escapeHtml(c.caliber)}</td>
                    <td style="padding: 10px 4px; text-align: right; font-weight: 500; font-variant-numeric: tabular-nums;">${c.count.toLocaleString()}</td>
                    <td style="padding: 10px 4px; text-align: right; font-weight: 600; font-variant-numeric: tabular-nums;">${formatCurrency(c.value_eur)}</td>
                `;
                caliberBody.appendChild(tr);
            });
        } else {
            caliberBody.innerHTML = `<tr><td colspan="3" style="text-align: center; color: var(--color-text-muted); padding: 20px;">No caliber data.</td></tr>`;
        }
    }

    // 5. Market integrity card
    const elEuActive = document.getElementById('ctx-eu-active');
    const elUsActive = document.getElementById('ctx-us-active');
    const elEurSold = document.getElementById('ctx-eur-sold');
    const elUsdSold = document.getElementById('ctx-usd-sold');
    if (elEuActive) elEuActive.textContent = market_summary.active_eu || 0;
    if (elUsActive) elUsActive.textContent = market_summary.active_us || 0;
    if (elEurSold) elEurSold.textContent = market_summary.sold_eur || 0;
    if (elUsdSold) elUsdSold.textContent = market_summary.sold_usd || 0;
}

// --- Render Inventory Page ---
// --- Helper Functions for Item Data Mapping ---
function getItemDisplayName(item) {
    const brand = item.brand || 'Vintage';
    const caliber = item.caliber && item.caliber !== 'unknown' && item.caliber !== 'None' ? item.caliber : '';
    const part = item.part_number || '';
    return `${brand}${caliber ? ' ' + caliber : ''} — ${part}`;
}

function getItemConfidence(item) {
    if (item.confidence_tier) {
        return item.confidence_tier.toUpperCase();
    }
    if (item.pricing_state) {
        const ps = item.pricing_state.toUpperCase();
        if (ps === 'GOVERNED' || ps === 'HIGH') return 'HIGH';
        if (ps === 'AUTO_CONFIRMED' || ps === 'HIGH_CONFIDENCE' || ps === 'MEDIUM') return 'MEDIUM';
    }
    if (item.recommendation_reason) {
        const m = item.recommendation_reason.match(/Confidence:\s*(LOW|MEDIUM|HIGH)/i);
        if (m) return m[1].toUpperCase();
    }
    return 'LOW';
}

function getItemTurnoverText(item) {
    if (item.sell_time_display) {
        return item.sell_time_display;
    }
    const days = item.median_days_to_sell;
    if (days === null || days === undefined || isNaN(days)) {
        return 'insufficient data';
    }
    return `${Math.round(days)} d`;
}

function getItemTurnoverConfidence(item) {
    const status = (item.turnover_evidence_status || '').toUpperCase();
    const method = (item.turnover_method || '').toUpperCase();
    const display = (item.sell_time_display || '').toLowerCase();
    if (
        status === 'NO_PRICE_RECOMMENDATION' ||
        method === 'NO_PRICE_RECOMMENDATION' ||
        display.includes('insufficient') ||
        display.includes('unavailable')
    ) {
        return 'INSUFFICIENT';
    }
    if (item.turnover_confidence) {
        return item.turnover_confidence.toUpperCase();
    }
    return 'INSUFFICIENT';
}

function getItemBasisText(item) {
    const b = item.valuation_basis || '';
    if (b === 'ACTIVE_ONLY') return 'Active-only';
    if (b === 'HISTORICAL') return 'Historical';
    if (b === 'ESTIMATED' || b === 'FALLBACK') return 'Estimated';
    // Fallback parsing from recommendation_reason
    if (item.recommendation_reason) {
        if (item.recommendation_reason.toLowerCase().includes('active asking prices only')) return 'Active-only';
        if (item.recommendation_reason.toLowerCase().includes('historical sold evidence')) return 'Historical';
    }
    return 'Estimated';
}

function updateSortIcons() {
    const cols = ['name', 'stock', 'tmv', 'confidence', 'turnover', 'basis'];
    cols.forEach(col => {
        const el = document.getElementById(`sort-icon-${col}`);
        if (!el) return;
        if (state.sort.column === col) {
            el.textContent = state.sort.ascending ? ' ▲' : ' ▼';
        } else {
            el.textContent = '';
        }
    });
}

// --- Render Inventory Page ---
function renderInventoryTable() {
    const tbody = document.getElementById('items-table-body');
    if (!state.itemsData) return;

    const { priced, unpriced } = state.itemsData;
    const allItems = [...priced, ...(unpriced || [])];
    const { search, brand, confidence, turnover, availability } = state.filters;

    // Apply filtering
    let filteredPriced = allItems.filter(item => {
        // Search term matching
        const searchStr = `${item.brand} ${item.caliber} ${item.part_number} ${item.canonical_inventory_id}`.toLowerCase();
        const matchesSearch = !search || searchStr.includes(search.toLowerCase());

        // Brand filtering
        const matchesBrand = brand === 'all' || item.brand === brand;

        // Confidence filtering
        const itemConf = getItemConfidence(item);
        const matchesConfidence = confidence === 'all' || itemConf === confidence;

        // Turnover confidence filtering
        const itemTurnoverConf = getItemTurnoverConfidence(item);
        const matchesTurnover = turnover === 'all' || itemTurnoverConf === turnover;

        // Availability filtering
        let matchesAvailability = true;
        if (availability === 'priced') {
            matchesAvailability = item.tmv_eur !== null && item.tmv_eur !== undefined;
        } else if (availability === 'unpriced') {
            matchesAvailability = item.tmv_eur === null || item.tmv_eur === undefined;
        }

        return matchesSearch && matchesBrand && matchesConfidence && matchesTurnover && matchesAvailability;
    });

    // Sort items
    filteredPriced.sort((a, b) => {
        let valA, valB;
        switch (state.sort.column) {
            case 'name':
                valA = getItemDisplayName(a).toLowerCase();
                valB = getItemDisplayName(b).toLowerCase();
                break;
            case 'stock':
                valA = a.stock || 0;
                valB = b.stock || 0;
                break;
            case 'tmv':
                const tmvA = a.tmv_eur;
                const tmvB = b.tmv_eur;
                valA = (tmvA === null || tmvA === undefined) ? -1 : tmvA;
                valB = (tmvB === null || tmvB === undefined) ? -1 : tmvB;
                break;
            case 'confidence':
                const confMap = { 'HIGH': 3, 'MEDIUM': 2, 'LOW': 1 };
                valA = confMap[getItemConfidence(a)] || 0;
                valB = confMap[getItemConfidence(b)] || 0;
                break;
            case 'turnover':
                // Nulls/insufficient data last (large value)
                const daysA = a.median_days_to_sell;
                const daysB = b.median_days_to_sell;
                valA = (daysA === null || daysA === undefined || isNaN(daysA)) ? 999999 : daysA;
                valB = (daysB === null || daysB === undefined || isNaN(daysB)) ? 999999 : daysB;
                break;
            case 'basis':
                valA = getItemBasisText(a).toLowerCase();
                valB = getItemBasisText(b).toLowerCase();
                break;
            default:
                valA = a.canonical_inventory_id;
                valB = b.canonical_inventory_id;
        }

        if (valA < valB) return state.sort.ascending ? -1 : 1;
        if (valA > valB) return state.sort.ascending ? 1 : -1;
        return 0;
    });

    if (filteredPriced.length === 0) {
        tbody.innerHTML = `<tr><td colspan="6" class="table-loading">No inventory items match filter requirements.</td></tr>`;
        return;
    }

    let tbodyHtml = '';
    filteredPriced.forEach(item => {
        const boldPartName = getItemDisplayName(item);
        const priceText = item.tmv_eur ? formatCurrency(item.tmv_eur) : '—';
        const turnoverText = getItemTurnoverText(item);
        
        // Confidence Headlight Class mapping (low, medium, high)
        const confidenceLabel = getItemConfidence(item);
        let confidenceClass = 'red'; // low
        if (confidenceLabel === 'HIGH') {
            confidenceClass = 'green';
        } else if (confidenceLabel === 'MEDIUM') {
            confidenceClass = 'yellow';
        }

        const basisText = getItemBasisText(item);

        tbodyHtml += `
            <tr onclick="navigateToItem('${item.canonical_inventory_id}')">
                <td>
                    <div class="part-id-cell">
                        <span class="part-main-name">${boldPartName}</span>
                    </div>
                </td>
                <td><strong>${item.stock || 0}</strong></td>
                <td class="price-cell">${priceText}</td>
                <td>
                    <span class="headlight-badge ${confidenceClass}">${confidenceLabel}</span>
                </td>
                <td class="turnover-cell">${turnoverText}</td>
                <td class="basis-cell" title="${escapeHtml(item.recommendation_reason || '')}">
                    ${basisText}
                </td>
            </tr>
        `;
    });

    tbody.innerHTML = tbodyHtml;
}

function renderUnpricedItems() {
    const container = document.getElementById('unpriced-list-body');
    const badge = document.getElementById('unpriced-count-badge');
    if (!state.itemsData) return;

    const { unpriced } = state.itemsData;
    badge.textContent = unpriced.length;

    if (unpriced.length === 0) {
        container.innerHTML = '<div style="grid-column: 1/-1; text-align: center; color: var(--color-text-muted); font-style: italic;">No items currently awaiting evidence. All inventory is priced.</div>';
        return;
    }

    let cardsHtml = '';
    unpriced.forEach(item => {
        const title = `${item.brand || ''} ${item.caliber || ''} - Part ${item.part_number || ''}`;
        cardsHtml += `
            <div class="unpriced-item-card">
                <div class="unpriced-card-title">${title}</div>
                <div class="unpriced-card-reason">${item.reason || 'Insufficient market evidence to recommend pricing.'}</div>
            </div>
        `;
    });

    container.innerHTML = cardsHtml;
}

function navigateToItem(itemId) {
    window.location.hash = `#item?id=${encodeURIComponent(itemId)}`;
}

// --- Render Detailed Item Page ---
function renderItemDetail() {
    if (!state.selectedItem) return;

    const { item, scenarios, price_time_table, active_matches, historical_matches } = state.selectedItem;

    // Reset simulator slider to default (0%)
    document.getElementById('price-simulator-slider').value = 0;
    document.getElementById('slider-percentage-val').textContent = '0%';

    // 1. Titles & Metadata
    document.getElementById('detail-title').textContent = `${item.brand || ''} ${item.caliber || ''} — Part ${item.part_number || ''}`;
    
    // Header badges
    document.getElementById('detail-badge-brand').textContent = item.brand || '—';
    document.getElementById('detail-badge-caliber').textContent = item.caliber || '—';
    document.getElementById('detail-badge-condition').textContent = item.condition || 'Used';
    document.getElementById('detail-badge-stock').textContent = `${item.stock || 0} unit(s)`;
    
    document.getElementById('detail-id').textContent = `Canonical ID: ${item.canonical_inventory_id}`;

    // 2. Confidence badge headlight mapping
    const badgeEl = document.getElementById('detail-confidence-badge');
    let confClass = 'red';
    const confidenceForBadge = getItemConfidence(item);
    if (confidenceForBadge === 'HIGH') {
        confClass = 'green';
    } else if (confidenceForBadge === 'MEDIUM') {
        confClass = 'yellow';
    }
    badgeEl.className = `headlight-badge ${confClass}`;
    badgeEl.textContent = item.pricing_state_label || `${confidenceForBadge} confidence`;

    // 3. Recommended Price card
    const priceMain = item.tmv_eur;
    document.getElementById('detail-price-main').textContent = priceMain ? formatCurrency(priceMain) : '—';

    // 4. Valuation Trace baseline numbers
    const H = item.historical_value_eur;
    const C = item.current_value_eur;
    const P = item.price_trend !== null && item.price_trend !== undefined ? parseFloat(item.price_trend) : 0.0;
    const S = item.market_dynamics !== null && item.market_dynamics !== undefined ? parseFloat(item.market_dynamics) : 0.50;

    const trendAdj = item.trend_adjustment_eur;
    const scarcityAdj = item.scarcity_adjustment_eur;

    document.getElementById('trace-h-val').textContent = H !== null && H !== undefined ? formatCurrency(H) : '—';
    document.getElementById('trace-c-val').textContent = C !== null && C !== undefined ? formatCurrency(C) : '—';
    
    // Update adjustments in traceback card
    const sAdjEl = document.getElementById('trace-s-val-adj');
    if (scarcityAdj !== null && scarcityAdj !== undefined) {
        sAdjEl.textContent = `${scarcityAdj >= 0 ? '+' : ''}${formatCurrency(scarcityAdj)}`;
        sAdjEl.className = `trace-value ${scarcityAdj >= 0 ? 'adjust-plus' : 'adjust-minus'}`;
    } else {
        sAdjEl.textContent = S !== null && S !== undefined ? `S = ${S.toFixed(2)}` : '—';
        sAdjEl.className = 'trace-value';
    }

    const tAdjEl = document.getElementById('trace-t-val-adj');
    if (trendAdj !== null && trendAdj !== undefined) {
        tAdjEl.textContent = `${trendAdj >= 0 ? '+' : ''}${formatCurrency(trendAdj)}`;
        tAdjEl.className = `trace-value ${trendAdj >= 0 ? 'adjust-plus' : 'adjust-minus'}`;
    } else {
        tAdjEl.textContent = P !== null && P !== undefined ? `P = ${(P * 100).toFixed(1)}%` : '—';
        tAdjEl.className = 'trace-value';
    }

    // Hidden legacy elements for compatibility
    document.getElementById('trace-p-val').textContent = `${(P * 100).toFixed(1)}%`;
    document.getElementById('trace-s-val').textContent = S.toFixed(2);
    document.getElementById('trace-final-val').textContent = priceMain ? formatCurrency(priceMain) : '—';

    // 5. Evidence depth summary counts (real matches only)
    const actCount = item.market_evidence_active || 0;
    const sldCount = item.market_evidence_sold || 0;
    document.getElementById('evidence-active-count').textContent = actCount;
    document.getElementById('evidence-sold-count').textContent = sldCount;

    // Reset drilldowns
    const activeDrilldown = document.getElementById('active-listings-drilldown');
    const historicalDrilldown = document.getElementById('historical-sales-drilldown');
    activeDrilldown.classList.add('hidden');
    historicalDrilldown.classList.add('hidden');
    activeDrilldown.innerHTML = '';
    historicalDrilldown.innerHTML = '';

    // Click handler for active listings
    document.getElementById('row-active-listings').onclick = () => {
        if (!activeDrilldown.classList.contains('hidden')) {
            activeDrilldown.classList.add('hidden');
        } else {
            historicalDrilldown.classList.add('hidden');
            if (!active_matches || active_matches.length === 0) {
                activeDrilldown.innerHTML = '<div class="evidence-no-matches">No active listings found.</div>';
            } else {
                activeDrilldown.innerHTML = active_matches.map(m => `
                    <a href="${m.url}" target="_blank" class="evidence-match-link" title="Click to view listing on eBay">
                        <span class="match-title">${escapeHtml(m.title)}</span>
                        <strong class="match-price">${m.price_eur !== null && m.price_eur !== undefined ? formatCurrency(m.price_eur) : '—'}</strong>
                    </a>
                `).join('');
            }
            activeDrilldown.classList.remove('hidden');
        }
    };

    // Click handler for historical sales
    document.getElementById('row-historical-sales').onclick = () => {
        if (!historicalDrilldown.classList.contains('hidden')) {
            historicalDrilldown.classList.add('hidden');
        } else {
            activeDrilldown.classList.add('hidden');
            if (!historical_matches || historical_matches.length === 0) {
                historicalDrilldown.innerHTML = '<div class="evidence-no-matches">No historical sales found.</div>';
            } else {
                historicalDrilldown.innerHTML = historical_matches.map(m => `
                    <a href="${m.url}" target="_blank" class="evidence-match-link" title="Click to view sale on eBay">
                        <span class="match-title">${escapeHtml(m.title)}</span>
                        <strong class="match-price">${m.price_eur !== null && m.price_eur !== undefined ? formatCurrency(m.price_eur) : '—'}</strong>
                    </a>
                `).join('');
            }
            historicalDrilldown.classList.remove('hidden');
        }
    };

    // 6. Expected Selling Time progress dot placement
    const turnoverDays = item.median_days_to_sell;
    const daysEl = document.getElementById('detail-turnover-days');
    if (item.sell_time_display && (!turnoverDays || item.sell_time_display.toLowerCase().includes('insufficient'))) {
        daysEl.textContent = item.sell_time_display;
        document.getElementById('timeline-dot-indicator').style.left = '0%';
        document.getElementById('timeline-fill-indicator').style.width = '0%';
    } else if (turnoverDays) {
        daysEl.textContent = item.sell_time_display || `${Math.round(turnoverDays)} days`;
        
        // Map expected days to timeline segments:
        let pct = 50; 
        const days = turnoverDays;
        if (days <= 7) {
            pct = (days / 7) * 20;
        } else if (days <= 30) {
            pct = 20 + ((days - 7) / 23) * 20;
        } else if (days <= 90) {
            pct = 40 + ((days - 30) / 60) * 20;
        } else if (days <= 183) {
            pct = 60 + ((days - 90) / 93) * 20;
        } else {
            pct = 80 + Math.min(20, ((days - 183) / 182) * 20);
        }
        pct = Math.max(0, Math.min(100, pct));
        
        document.getElementById('timeline-dot-indicator').style.left = `${pct}%`;
        document.getElementById('timeline-fill-indicator').style.width = `${pct}%`;
    } else {
        daysEl.textContent = '—';
        document.getElementById('timeline-dot-indicator').style.left = '0%';
        document.getElementById('timeline-fill-indicator').style.width = '0%';
    }

    // 7. Scenarios comparisons initial display
    if (scenarios) {
        // US (A)
        const scA = scenarios.A;
        document.getElementById('scen-a-landed').textContent = formatCurrency(scA.landed_cost_eur);
        document.getElementById('scen-a-base').textContent = formatCurrency(scA.price_eur);
        document.getElementById('scen-a-ship').textContent = formatCurrency(scA.shipping_eur);
        document.getElementById('scen-a-customs').textContent = formatCurrency(scA.customs_eur);
        document.getElementById('scen-a-tax').textContent = formatCurrency(scA.tax_eur);

        // DE (B)
        const scB = scenarios.B;
        document.getElementById('scen-b-landed').textContent = formatCurrency(scB.landed_cost_eur);
        document.getElementById('scen-b-base').textContent = formatCurrency(scB.price_eur);
        document.getElementById('scen-b-ship').textContent = formatCurrency(scB.shipping_eur);
        document.getElementById('scen-b-customs').textContent = formatCurrency(scB.customs_eur);

        // Virtual (C)
        const scC = scenarios.C;
        document.getElementById('scen-c-landed').textContent = formatCurrency(scC.landed_cost_eur);
    }

    // 8. Initial simulator setup
    calculateClientSimulation(0);
}

// --- Dynamic Slider Simulator Calculator ---
function calculateClientSimulation(percentage) {
    if (!state.selectedItem) return;
    const { item, scenarios, price_time_table } = state.selectedItem;

    const baseTmv = item.tmv_eur;
    const baseDays = item.median_days_to_sell;
    const stock = item.stock || 0;

    if (!baseTmv || !baseDays) {
        document.getElementById('sim-price-val').textContent = '—';
        document.getElementById('slider-days-val').textContent = '—';
        document.getElementById('sim-units-sold').textContent = '—';
        document.getElementById('sim-revenue-val').textContent = '—';
        
        document.getElementById('fc-sold-30-val').textContent = '—';
        document.getElementById('fc-rev-30-val').textContent = '€—';
        document.getElementById('fc-sold-30-90-val').textContent = '—';
        document.getElementById('fc-rev-30-90-val').textContent = '€—';
        document.getElementById('fc-sold-90-plus-val').textContent = '—';
        document.getElementById('fc-rev-90-plus-val').textContent = '€—';
        return;
    }

    // Extract epsilon from server price_time_table response if present, otherwise default to 1.5
    let epsilon = 1.5;
    if (price_time_table && price_time_table.length > 0) {
        epsilon = price_time_table[0].epsilon || 1.5;
    }

    // Compute Simulated Price
    const factor = 1 + (percentage / 100);
    const simPrice = baseTmv * factor;

    // Compute Simulated Days using power law formula
    const timeFactor = Math.pow(factor, epsilon);
    const simDays = baseDays * timeFactor;
    
    // Revenue
    const simRevenue = simPrice * stock;

    // Update display values with smooth formatting
    document.getElementById('sim-pct-change').textContent = `${percentage > 0 ? '+' : ''}${percentage}%`;
    document.getElementById('sim-price-val').textContent = formatCurrency(simPrice);
    document.getElementById('slider-days-val').textContent = `${Math.round(simDays)} days`;
    document.getElementById('sim-revenue-val').textContent = formatCurrency(simRevenue);

    // Visual helper slider position adjustment
    const sellingTimeSlider = document.getElementById('selling-time-slider');
    if (sellingTimeSlider) {
        // Map 50 as baseline baseDays.
        // If simDays is 2x baseline, value goes to 0 (Slow). If simDays is 0.5x baseline, value goes to 100 (Fast).
        const ratio = baseDays / simDays;
        let sliderVal = 50 * ratio;
        sliderVal = Math.max(0, Math.min(100, sliderVal));
        sellingTimeSlider.value = sliderVal;
    }

    // Compute simulated sell probabilities using exponential survival curve: p = 1 - 2^(-t / median_days)
    let simP30 = item.prob_sell_30d || 0;
    let simP90 = item.prob_sell_90d || 0;
    if (percentage !== 0 && simDays > 0) {
        simP30 = 1 - Math.pow(2, -30 / simDays);
        simP90 = 1 - Math.pow(2, -90 / simDays);
    }

    // Run item-level Monte Carlo simulation (2,000 trials)
    const simStats = runSingleItemSimulation(stock, simP30, simP90, simPrice);

    // Update Expected Units Sold metric
    document.getElementById('sim-units-sold').textContent = `${formatUnitCount(simStats.days90.mean)} / ${formatUnitCount(stock)}`;

    // Update scenarios base and taxes dynamically
    if (scenarios) {
        const scA = scenarios.A;
        const scATaxableBase = (scA.price_eur || 0) + (scA.shipping_eur || 0) + (scA.customs_eur || 0);
        const scACustomsBase = (scA.price_eur || 0) + (scA.shipping_eur || 0);
        const customsRatio = scACustomsBase > 0 ? ((scA.customs_eur || 0) / scACustomsBase) : 0;
        const taxRatio = scATaxableBase > 0 ? ((scA.tax_eur || 0) / scATaxableBase) : 0;
        const simScABase = simPrice;
        const simScACustoms = (simScABase + (scA.shipping_eur || 0)) * customsRatio;
        const simScATax = (simScABase + (scA.shipping_eur || 0) + simScACustoms) * taxRatio;
        const simScALanded = simScABase + scA.shipping_eur + simScACustoms + simScATax;
        
        document.getElementById('scen-a-base').textContent = formatCurrency(simScABase);
        document.getElementById('scen-a-customs').textContent = formatCurrency(simScACustoms);
        document.getElementById('scen-a-tax').textContent = formatCurrency(simScATax);
        document.getElementById('scen-a-landed').textContent = formatCurrency(simScALanded);

        const scB = scenarios.B;
        const scBCustomsBase = (scB.price_eur || 0) + (scB.shipping_eur || 0);
        const customsRatioB = scBCustomsBase > 0 ? ((scB.customs_eur || 0) / scBCustomsBase) : 0;
        const simScBBase = simPrice;
        const simScBCustoms = (simScBBase + (scB.shipping_eur || 0)) * customsRatioB;
        const simScBLanded = simScBBase + scB.shipping_eur + simScBCustoms;
        
        document.getElementById('scen-b-base').textContent = formatCurrency(simScBBase);
        document.getElementById('scen-b-customs').textContent = formatCurrency(simScBCustoms);
        document.getElementById('scen-b-landed').textContent = formatCurrency(simScBLanded);

        document.getElementById('scen-c-landed').textContent = formatCurrency(simPrice);
    }

    // Populate Turnover Forecast Table (Dynamic Monte Carlo horizons)
    const sold30 = simStats.days30.mean;
    const rev30 = simStats.days30.revenue;
    const sold90 = simStats.days90.mean;
    const rev90 = simStats.days90.revenue;

    const sold30_90 = Math.max(0, sold90 - sold30);
    const rev30_90 = Math.max(0, rev90 - rev30);
    const sold90Plus = Math.max(0, stock - sold90);
    const rev90Plus = Math.max(0, sold90Plus * simPrice);

    document.getElementById('fc-sold-30-val').textContent = formatUnitCount(sold30);
    document.getElementById('fc-rev-30-val').textContent = formatCurrency(rev30);
    
    document.getElementById('fc-sold-30-90-val').textContent = formatUnitCount(sold30_90);
    document.getElementById('fc-rev-30-90-val').textContent = formatCurrency(rev30_90);
    
    document.getElementById('fc-sold-90-plus-val').textContent = formatUnitCount(sold90Plus);
    document.getElementById('fc-rev-90-plus-val').textContent = formatCurrency(rev90Plus);

    // Update Forecast footer
    document.getElementById('fc-remaining-units').textContent = `${formatUnitCount(sold90Plus)} unit(s) remaining`;
    document.getElementById('fc-potential-revenue').textContent = formatCurrency(rev30 + rev30_90 + rev90Plus);

    // 9. Update Business Recommendations list dynamically
    updateBusinessRecommendations(item, percentage, simPrice, simDays);
}

// --- Dynamic Business Recommendation Bullets Generator ---
function updateBusinessRecommendations(item, percentage, simPrice, simDays) {
    const list = document.getElementById('business-recommendations-list');
    if (!list) return;

    const bullets = [];
    
    // 1. Evidence / Confidence Bullet
    const confidence = getItemConfidence(item);
    const basis = getItemBasisText(item);
    if (confidence === 'HIGH') {
        bullets.push(`${basis} evidence supports a high-confidence recommended price.`);
    } else if (confidence === 'MEDIUM') {
        bullets.push(`${basis} evidence supports a medium-confidence price estimate.`);
    } else {
        bullets.push('Low evidence depth. Treat this as a cautious recommendation and monitor fresh market evidence.');
    }

    // 2. Market Dynamics / Scarcity Bullet
    const scarcity = item.market_dynamics;
    if (scarcity !== null && scarcity !== undefined) {
        if (scarcity > 0.6) {
            bullets.push(`High scarcity (S = ${parseFloat(scarcity).toFixed(2)}) indicates low supplier competition.`);
        } else if (scarcity < 0.4) {
            bullets.push(`Oversupplied market (S = ${parseFloat(scarcity).toFixed(2)}). Price defensively to increase sell-through.`);
        } else {
            bullets.push('Balanced market demand and supply availability.');
        }
    } else {
        bullets.push('Market balance indicator is unavailable for this item.');
    }

    // 3. Pricing Bullet
    if (percentage < -5) {
        bullets.push(`Priced competitively at a discount (${percentage}%) for faster velocity.`);
    } else if (percentage > 5) {
        bullets.push(`Priced at a premium (+${percentage}%) to maximize margin.`);
    } else {
        bullets.push('Priced competitively for optimal sell-through.');
    }

    // 4. Turnover Window Bullet
    if (!simDays || isNaN(simDays)) {
        bullets.push(item.sell_time_display || 'Sell-time estimate unavailable from current evidence.');
    } else if (simDays <= 30) {
        bullets.push('Recommended to list immediately for fast turnover.');
    } else if (simDays > 30 && simDays <= 90) {
        bullets.push('Recommended to list in the 31-90 day window.');
    } else {
        bullets.push(`Extended expected selling period (${Math.round(simDays)} days). Monitor stock holding costs.`);
    }

    // 5. Trend Bullet
    const trend = item.price_trend;
    if (trend !== null && trend !== undefined && Math.abs(trend) > 0.01) {
        if (trend > 0) {
            bullets.push(`Upward price trend (P = +${(trend * 100).toFixed(1)}%). Monitor market for potential price increase.`);
        } else {
            bullets.push(`Downward price trend (P = ${(trend * 100).toFixed(1)}%). Review stock holdings.`);
        }
    } else {
        bullets.push('Monitor market for potential price adjustments.');
    }

    // Render list
    list.innerHTML = bullets.map(b => `
        <li>
            <span class="check-icon">✓</span>
            <span class="rec-text">${escapeHtml(b)}</span>
        </li>
    `).join('');
}

function runSingleItemSimulation(stock, p30, p90, price) {
    if (stock <= 0) {
        return {
            days30: { mean: 0, p5: 0, p95: 0, revenue: 0 },
            days90: { mean: 0, p5: 0, p95: 0, revenue: 0 }
        };
    }
    
    const trials = 2000;
    const s30Runs = [];
    const s90Runs = [];
    let sold30Total = 0;
    let sold90Total = 0;
    
    for (let t = 0; t < trials; t++) {
        let s30 = 0;
        let s90 = 0;
        for (let i = 0; i < stock; i++) {
            if (Math.random() < p30) s30++;
            if (Math.random() < p90) s90++;
        }
        s30Runs.push(s30);
        s90Runs.push(s90);
        sold30Total += s30;
        sold90Total += s90;
    }
    
    s30Runs.sort((a, b) => a - b);
    s90Runs.sort((a, b) => a - b);
    
    const mean30 = sold30Total / trials;
    const mean90 = sold90Total / trials;
    
    // Percentiles (5% and 95%)
    const p5_30 = s30Runs[Math.floor(trials * 0.05)];
    const p95_30 = s30Runs[Math.floor(trials * 0.95)];
    const p5_90 = s90Runs[Math.floor(trials * 0.05)];
    const p95_90 = s90Runs[Math.floor(trials * 0.95)];
    
    return {
        days30: {
            mean: mean30,
            p5: p5_30,
            p95: p95_30,
            revenue: mean30 * price
        },
        days90: {
            mean: mean90,
            p5: p5_90,
            p95: p95_90,
            revenue: mean90 * price
        }
    };
}

// --- Helper Functions ---
function formatCurrency(value) {
    if (value === null || value === undefined || isNaN(value)) return '—';
    return new Intl.NumberFormat('de-DE', { style: 'currency', currency: 'EUR' }).format(value);
}

function formatUnitCount(value) {
    if (value === null || value === undefined || isNaN(value)) return '—';
    return Math.round(Number(value)).toLocaleString();
}

function allocateWholeUnits(values, targetTotal) {
    const target = Math.max(0, Math.round(Number(targetTotal) || 0));
    const floors = values.map(value => Math.floor(Math.max(0, Number(value) || 0)));
    let remainder = target - floors.reduce((sum, value) => sum + value, 0);
    const order = values
        .map((value, index) => ({
            index,
            frac: Math.max(0, Number(value) || 0) - floors[index],
        }))
        .sort((a, b) => b.frac - a.frac);

    for (let i = 0; i < order.length && remainder > 0; i += 1) {
        floors[order[i].index] += 1;
        remainder -= 1;
    }
    return floors;
}

function escapeHtml(unsafe) {
    return unsafe
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}

// --- Add Item / Update Stock Page Logic ---
function initAddItemPage() {
    if (state.addItemPageInitialized) {
        return;
    }
    state.addItemPageInitialized = true;

    const tabBtnSingle = document.getElementById('tab-btn-single');
    const tabBtnBulk = document.getElementById('tab-btn-bulk');
    const tabContentSingle = document.getElementById('tab-content-single');
    const tabContentBulk = document.getElementById('tab-content-bulk');

    // Tab toggle event listeners
    tabBtnSingle.addEventListener('click', () => {
        tabBtnSingle.classList.add('active');
        tabBtnBulk.classList.remove('active');
        tabContentSingle.classList.remove('hidden');
        tabContentBulk.classList.add('hidden');
    });

    tabBtnBulk.addEventListener('click', () => {
        tabBtnBulk.classList.add('active');
        tabBtnSingle.classList.remove('active');
        tabContentBulk.classList.remove('hidden');
        tabContentSingle.classList.add('hidden');
    });

    // --- Tab 1: Single Item Manager Logic ---
    const formSingle = document.getElementById('single-item-form');
    const brandInput = document.getElementById('single-brand');
    const caliberInput = document.getElementById('single-caliber');
    const partNumberInput = document.getElementById('single-part-number');
    const stockInput = document.getElementById('single-stock');
    const stockLabel = document.getElementById('single-stock-label');
    const stockHelp = document.getElementById('single-stock-help');
    const stockModeGroup = document.getElementById('stock-mode-group');
    const stockModeInput = document.getElementById('stock-mode');
    const btnSubmit = document.getElementById('btn-single-submit');
    const statusContent = document.getElementById('single-status-content');

    let checkTimeout = null;
    let singleSubmitInFlight = false;
    let currentItemExists = false;
    let currentStock = 0;

    const checkItemStatus = () => {
        const brand = brandInput.value;
        const caliber = caliberInput.value.trim();
        const partNumber = partNumberInput.value.trim();

        if (!caliber || !partNumber) {
            currentItemExists = false;
            currentStock = 0;
            stockModeGroup.classList.add('hidden');
            stockLabel.textContent = 'Initial Stock Quantity';
            stockHelp.textContent = 'For a new item, enter the starting physical stock.';
            statusContent.innerHTML = `
                <div class="status-placeholder">
                    <span class="status-icon">ℹ</span>
                    <p>Please enter Calibre and Part Number to check/add items.</p>
                </div>
            `;
            btnSubmit.disabled = true;
            btnSubmit.textContent = 'Add Item';
            return;
        }

        // Query backend `/api/check_item`
        const url = `/api/check_item?brand=${encodeURIComponent(brand)}&caliber=${encodeURIComponent(caliber)}&part_number=${encodeURIComponent(partNumber)}`;
        
        fetch(url)
            .then(res => res.json())
            .then(data => {
                if (data.exists) {
                    currentItemExists = true;
                    currentStock = Number(data.current_stock || 0);
                    stockModeGroup.classList.remove('hidden');
                    stockLabel.textContent = 'Stock Quantity';
                    stockHelp.textContent = stockModeInput.value === 'add'
                        ? `This will add units to the current stock (${currentStock}).`
                        : `This will replace the current total stock (${currentStock}).`;
                    statusContent.innerHTML = `
                        <div class="status-info-card">
                            <strong>ℹ Item already in the inventory (Current Stock: ${data.current_stock})</strong>
                            <p style="margin-top: 5px; font-size: 0.9em; color: var(--color-text-muted);">
                                Choose whether to set the total stock or add units to the current stock.
                            </p>
                        </div>
                        <div style="margin-top: 15px; font-size: 0.9em;">
                            <strong>Canonical ID:</strong> <code>${data.canonical_id}</code>
                        </div>
                    `;
                    if (stockModeInput.value === 'set') {
                        stockInput.value = data.current_stock;
                    }
                    btnSubmit.textContent = stockModeInput.value === 'add' ? 'Add Units' : 'Update Stock';
                    btnSubmit.disabled = false;
                } else {
                    currentItemExists = false;
                    currentStock = 0;
                    stockModeGroup.classList.add('hidden');
                    stockLabel.textContent = 'Initial Stock Quantity';
                    stockHelp.textContent = 'For a new item, enter the starting physical stock.';
                    statusContent.innerHTML = `
                        <div class="status-info-card" style="border-left-color: #10B981;">
                            <strong style="color: #047857;">✓ Available: This is a new part number</strong>
                            <p style="margin-top: 5px; font-size: 0.9em; color: var(--color-text-muted);">
                                This part does not exist in staging. Submitting will create a new raw entry.
                            </p>
                        </div>
                        <div style="margin-top: 15px; font-size: 0.9em;">
                            <strong>Canonical ID (Proposed):</strong> <code>${data.canonical_id}</code>
                        </div>
                    `;
                    btnSubmit.textContent = 'Add Item';
                    btnSubmit.disabled = false;
                }
            })
            .catch(err => {
                console.error("Error checking item status:", err);
                statusContent.innerHTML = `
                    <div class="status-error-card">
                        Failed to check item status. Is the server online?
                    </div>
                `;
            });
    };

    // Auto-check status when user types
    const debouncedCheck = () => {
        if (checkTimeout) clearTimeout(checkTimeout);
        checkTimeout = setTimeout(checkItemStatus, 300);
    };

    stockModeInput.addEventListener('change', () => {
        if (!currentItemExists) return;
        if (stockModeInput.value === 'set') {
            stockInput.value = currentStock;
            stockHelp.textContent = `This will replace the current total stock (${currentStock}).`;
            btnSubmit.textContent = 'Update Stock';
        } else {
            stockInput.value = 0;
            stockHelp.textContent = `This will add units to the current stock (${currentStock}). Example: add 37 to move from 1 to 38.`;
            btnSubmit.textContent = 'Add Units';
        }
    });

    brandInput.addEventListener('change', checkItemStatus);
    caliberInput.addEventListener('input', debouncedCheck);
    partNumberInput.addEventListener('input', debouncedCheck);

    // Form submit listener
    formSingle.addEventListener('submit', (e) => {
        e.preventDefault();
        if (singleSubmitInFlight) return;
        
        const brand = brandInput.value;
        const caliber = caliberInput.value.trim();
        const partNumber = partNumberInput.value.trim();
        const stock = parseInt(stockInput.value) || 0;
        const stockMode = currentItemExists ? stockModeInput.value : 'set';

        if (!caliber || !partNumber) return;

        singleSubmitInFlight = true;
        btnSubmit.disabled = true;
        btnSubmit.textContent = 'Processing...';

        fetch('/api/add_or_update_item', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ brand, caliber, part_number: partNumber, stock, stock_mode: stockMode })
        })
        .then(res => res.json())
        .then(data => {
            if (data.success) {
                statusContent.innerHTML = `
                    <div class="status-success-card">
                        ${data.message}
                    </div>
                `;
                if (data.job_id) {
                    pollPipelineJob(data.job_id, statusContent, data.canonical_id || null);
                }
                setTimeout(() => {
                    singleSubmitInFlight = false;
                    checkItemStatus();
                }, 2000);

                // Reload global items data
                fetchItems();
                fetchPortfolio();
            } else {
                statusContent.innerHTML = `
                    <div class="status-error-card">
                        ${data.error || 'Failed to update item.'}
                    </div>
                `;
                singleSubmitInFlight = false;
                btnSubmit.disabled = false;
            }
        })
        .catch(err => {
            console.error("Error submitting single item:", err);
            statusContent.innerHTML = `
                <div class="status-error-card">
                    Network error occurred while updating the item.
                </div>
            `;
            singleSubmitInFlight = false;
            btnSubmit.disabled = false;
        });
    });

    // --- Tab 2: Bulk CSV Update Logic ---
    const fileInput = document.getElementById('bulk-file-input');
    const fileInfo = document.getElementById('bulk-file-info');
    const btnBulkSubmit = document.getElementById('btn-bulk-submit');
    const bulkStatus = document.getElementById('bulk-status-container');
    const validationPanel = document.getElementById('bulk-validation-panel');

    fileInput.addEventListener('change', (e) => {
        const file = e.target.files[0];
        if (file) {
            fileInfo.textContent = `Selected File: ${file.name} (${(file.size / 1024).toFixed(2)} KB)`;
            
            // Start validation dry-run immediately
            btnBulkSubmit.disabled = true;
            validationPanel.classList.remove('hidden');
            validationPanel.style.borderColor = '#e2e8f0';
            validationPanel.style.backgroundColor = '#f8fafc';
            validationPanel.innerHTML = '<strong style="color:#475569">Running validation check on CSV file...</strong>';
            
            const reader = new FileReader();
            reader.onload = function(evt) {
                const csvContent = evt.target.result;
                fetch('/api/validate_bulk', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ filename: file.name, content: csvContent })
                })
                .then(res => res.json())
                .then(data => {
                    if (data.error) {
                        validationPanel.style.borderColor = '#fca5a5';
                        validationPanel.style.backgroundColor = '#fef2f2';
                        validationPanel.innerHTML = `<strong style="color:#b91c1c">CSV format check failed:</strong><p style="color:#b91c1c; margin-top:5px;">${escapeHtml(data.error)}</p>`;
                        btnBulkSubmit.disabled = true;
                        return;
                    }
                    if (data.ok) {
                        validationPanel.style.borderColor = '#86efac';
                        validationPanel.style.backgroundColor = '#f0fdf4';
                        validationPanel.innerHTML = `
                            <strong style="color:#15803d">✓ Upload check passed:</strong>
                            <p style="color:#15803d; margin-top:5px; font-size:0.95em;">
                                ${data.new_rows} new row(s), ${data.stock_updates} stock update(s).
                            </p>
                        `;
                        btnBulkSubmit.disabled = false;
                    } else {
                        validationPanel.style.borderColor = '#fca5a5';
                        validationPanel.style.backgroundColor = '#fef2f2';
                        let errorsHtml = `<strong style="color:#b91c1c">✗ Upload check failed. No rows will be written until these issues are fixed:</strong><ul style="margin-top:8px; padding-left:20px; color:#b91c1c; font-size:0.9em; text-align:left;">`;
                        const displayErrors = data.errors.slice(0, 10);
                        displayErrors.forEach(err => {
                            errorsHtml += `<li style="margin-bottom:3px">${escapeHtml(err)}</li>`;
                        });
                        if (data.errors.length > 10) {
                            errorsHtml += `<li style="list-style:none; font-style:italic; margin-top:5px;">... and ${data.errors.length - 10} more issue(s).</li>`;
                        }
                        errorsHtml += `</ul>`;
                        validationPanel.innerHTML = errorsHtml;
                        btnBulkSubmit.disabled = true;
                    }
                })
                .catch(err => {
                    console.error("Error validating CSV:", err);
                    validationPanel.style.borderColor = '#fca5a5';
                    validationPanel.style.backgroundColor = '#fef2f2';
                    validationPanel.innerHTML = `<strong style="color:#b91c1c">Connection error occurred while checking CSV validation.</strong>`;
                    btnBulkSubmit.disabled = true;
                });
            };
            reader.readAsText(file);
        } else {
            fileInfo.textContent = 'No file selected';
            btnBulkSubmit.disabled = true;
            validationPanel.classList.add('hidden');
            validationPanel.innerHTML = '';
        }
    });

    btnBulkSubmit.addEventListener('click', () => {
        const file = fileInput.files[0];
        if (!file) return;

        btnBulkSubmit.disabled = true;
        btnBulkSubmit.textContent = 'Confirming Import...';
        bulkStatus.className = 'status-info-card';
        bulkStatus.innerHTML = '<strong>Processing CSV update file. Please standby...</strong>';
        bulkStatus.classList.remove('hidden');

        const reader = new FileReader();
        reader.onload = function(evt) {
            const csvContent = evt.target.result;

            fetch('/api/bulk_update', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ filename: file.name, content: csvContent })
            })
            .then(res => res.json())
            .then(data => {
                if (data.success) {
                    bulkStatus.className = 'status-success-card';
                    bulkStatus.innerHTML = `<strong>Bulk Update Succeeded!</strong><p>${data.message}</p>`;
                    if (data.job_ids && data.job_ids.length > 0) {
                        bulkStatus.innerHTML += `<p>${data.job_ids.length} pricing job(s) queued. New item recommendations will appear as jobs finish.</p>`;
                    }
                    
                    // Reset input
                    fileInput.value = '';
                    fileInfo.textContent = 'No file selected';
                    validationPanel.classList.add('hidden');
                    validationPanel.innerHTML = '';
                    
                    // Reload global items data
                    fetchItems();
                    fetchPortfolio();
                } else {
                    bulkStatus.className = 'status-error-card';
                    bulkStatus.innerHTML = `<strong>Bulk Update Failed!</strong><p>${data.error || 'Unknown error'}</p>`;
                    btnBulkSubmit.disabled = false;
                }
                btnBulkSubmit.textContent = 'Confirm Import';
            })
            .catch(err => {
                console.error("Error posting bulk update:", err);
                bulkStatus.className = 'status-error-card';
                bulkStatus.innerHTML = '<strong>Network error occurred during bulk update transaction.</strong>';
                btnBulkSubmit.disabled = false;
                btnBulkSubmit.textContent = 'Confirm Import';
            });
        };
        reader.readAsText(file);
    });
}

function escapeCSVValue(val) {
    if (val === null || val === undefined) return '';
    const str = String(val);
    if (/[",\r\n]/.test(str)) {
        return `"${str.replace(/"/g, '""')}"`;
    }
    return str;
}

function downloadInventoryCSV() {
    if (!state.itemsData) {
        alert('Inventory data is still loading. Please try again in a moment.');
        return;
    }
    const { priced, unpriced } = state.itemsData;
    const allItems = [...priced, ...(unpriced || [])];

    const headers = [
        'Canonical ID',
        'Brand',
        'Caliber',
        'Part Number',
        'Stock',
        'Pricing Status',
        'Confidence Tier',
        'TMV (EUR)',
        'TMV Low (EUR)',
        'TMV High (EUR)',
        'Valuation Basis',
        'Median Days to Sell',
        'Market Evidence (Active)',
        'Market Evidence (Sold)',
        'Recommendation Reason / Status Note'
    ];

    const rows = allItems.map(item => {
        const pricingStatus = item.pricing_state === 'GOVERNED' 
            ? 'Pricing Ready — Validated' 
            : (item.pricing_state === 'AUTO_CONFIRMED' 
                ? 'Pricing Ready' 
                : (item.pricing_state === 'HIGH_CONFIDENCE' 
                    ? 'Pricing Estimate' 
                    : 'Awaiting Evidence'));

        const confidence = item.pricing_state === 'UNPRICED' ? 'N/A' : getItemConfidence(item);
        const valuationBasis = item.pricing_state === 'UNPRICED' ? 'Awaiting Evidence' : getItemBasisText(item);

        return [
            item.canonical_inventory_id,
            item.brand || '',
            item.caliber || '',
            item.part_number || '',
            item.stock || 0,
            pricingStatus,
            confidence,
            item.tmv_eur !== null && item.tmv_eur !== undefined ? item.tmv_eur : '',
            item.tmv_low_eur !== null && item.tmv_low_eur !== undefined ? item.tmv_low_eur : '',
            item.tmv_high_eur !== null && item.tmv_high_eur !== undefined ? item.tmv_high_eur : '',
            valuationBasis,
            item.median_days_to_sell !== null && item.median_days_to_sell !== undefined ? Math.round(item.median_days_to_sell) : '',
            item.market_evidence_active !== null && item.market_evidence_active !== undefined ? item.market_evidence_active : 0,
            item.market_evidence_sold !== null && item.market_evidence_sold !== undefined ? item.market_evidence_sold : 0,
            item.recommendation_reason || item.reason || ''
        ];
    });

    const csvContent = [
        headers.map(escapeCSVValue).join(','),
        ...rows.map(row => row.map(escapeCSVValue).join(','))
    ].join('\r\n');

    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.setAttribute('href', url);
    link.setAttribute('download', `watch_parts_inventory_${new Date().toISOString().slice(0, 10)}.csv`);
    link.style.visibility = 'hidden';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
}
