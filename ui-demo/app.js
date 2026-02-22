// Use real data if provided by Streamlit, otherwise fallback to mock data
const tenants = (window.DASHBOARD_DATA && window.DASHBOARD_DATA.tenants) ? window.DASHBOARD_DATA.tenants : [
    { name: "佐藤 一郎", property: "P-101", rent: 85000, balance: 0, status: "支払い済み" },
    { name: "田中 花子", property: "P-105", rent: 120000, balance: 240000, status: "支払い遅延" },
    { name: "鈴木 二郎", property: "P-202", rent: 95000, balance: 0, status: "支払い済み" },
    { name: "高橋 健", property: "P-301", rent: 78000, balance: 78000, status: "支払い遅延" },
    { name: "渡辺 結衣", "property": "P-404", rent: 150000, balance: 0, status: "支払い済み" },
    { name: "伊藤 真司", property: "P-505", rent: 110000, balance: 0, status: "支払い済み" }
];

const metrics = (window.DASHBOARD_DATA && window.DASHBOARD_DATA.metrics) ? window.DASHBOARD_DATA.metrics : {
    collectedRent: 2480000,
    expectedRent: 2580000,
    overdueCount: 5,
    activeTenants: 124,
    occupancyRate: 94,
    targetPct: 98
};

function animateValue(obj, start, end, duration, isCurrency = false) {
    let startTimestamp = null;
    const step = (timestamp) => {
        if (!startTimestamp) startTimestamp = timestamp;
        const progress = Math.min((timestamp - startTimestamp) / duration, 1);
        const value = Math.floor(progress * (end - start) + start);
        obj.innerHTML = isCurrency ? "¥" + value.toLocaleString() : value.toLocaleString();
        if (progress < 1) {
            window.requestAnimationFrame(step);
        }
    };
    window.requestAnimationFrame(step);
}

function renderTenants() {
    const list = document.getElementById('tenant-list');
    if (!list) return;
    list.innerHTML = '';

    tenants.forEach(t => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${t.name}</td>
            <td>${t.property}</td>
            <td class="font-mono">¥${t.rent.toLocaleString()}</td>
            <td class="font-mono ${t.balance > 0 ? 'gold' : ''}">¥${t.balance.toLocaleString()}</td>
            <td>
                <span class="status-badge ${t.status === '支払い済み' ? 'status-paid' : 'status-overdue'}">
                    ${t.status}
                </span>
            </td>
        `;
        list.appendChild(row);
    });
}

function initDashboard() {
    // Update Metrics with animation
    const metricElems = document.querySelectorAll('.metric-value');
    if (metricElems.length >= 4) {
        animateValue(metricElems[0], 0, metrics.collectedRent, 1500, true);
        animateValue(metricElems[1], 0, metrics.expectedRent, 1500, true);
        animateValue(metricElems[2], 0, metrics.overdueCount, 1500);
        animateValue(metricElems[3], 0, metrics.activeTenants, 1500);
    }

    // Update Detail Text
    const changes = document.querySelectorAll('.metric-change span');
    if (changes.length >= 4) {
        changes[0].textContent = `目標の ${metrics.targetPct}%`;
        changes[3].textContent = `入居率 ${metrics.occupancyRate}%`;
    }

    renderTenants();
}

document.addEventListener('DOMContentLoaded', initDashboard);
