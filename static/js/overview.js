// We'll keep a global array of all rows we have retrieved
let allRows = [];

/**
 * Toggling "Select All" checkbox to check/uncheck all row checkboxes
 */
function toggleSelectAll(selectAllEl) {
    console.log("Toggle Select All:", selectAllEl.checked);
    const checkboxes = document.querySelectorAll('.row-checkbox');
    checkboxes.forEach(chk => {
        chk.checked = selectAllEl.checked;
        console.log("Checkbox toggled:", chk);
    });
}

/**
 * Page-level method:
 * Gather selected row IDs and open or POST to the endpoint
 */
function handlePageMethod(endpoint) {
    console.log("Handling page method with endpoint:", endpoint);
    const checkboxes = document.querySelectorAll('.row-checkbox:checked');
    const selectedIds = Array.from(checkboxes).map(chk => chk.getAttribute('data-row-id'));
    console.log("Selected IDs:", selectedIds);

    if (!selectedIds.length) {
        alert("No rows selected");
        console.warn("No rows selected for page method.");
        return;
    }

    const joinedIds = selectedIds.join(',');
    console.log("Joined IDs for action:", joinedIds);

    const url = `${endpoint}?value=${joinedIds}`;
    console.log("Redirecting to URL:", url);
    window.location.href = url;
}


/**
 * Apply filters: gather the filter values, reset offset=0, fetch again
 */
function applyFilters() {
    console.log("Applying filters.");
    document.getElementById('offset').value = 0;
    fetchData();
}

/**
 * Go to previous page
 */
function prevPage() {
    console.log("Going to previous page.");
    let offset = parseInt(document.getElementById('offset').value, 10);
    const limit = parseInt(document.getElementById('limit').value, 10);
    offset = Math.max(offset - limit, 0);
    document.getElementById('offset').value = offset;
    console.log("New offset after prevPage:", offset);
    fetchData();
}

/**
 * Go to next page
 */
function nextPage() {
    console.log("Going to next page.");
    let offset = parseInt(document.getElementById('offset').value, 10);
    const limit = parseInt(document.getElementById('limit').value, 10);
    offset += limit;
    document.getElementById('offset').value = offset;
    console.log("New offset after nextPage:", offset);
    fetchData();
}

/**
 * Build a form-like object from filters + offset/limit and POST to /fetch_data
 * Then update table with the new rows, preserve checkboxes if needed, etc.
 */
async function fetchData() {
    console.log("Fetching data.");
    const tableId = document.getElementById('tableId').value;
    let offset = parseInt(document.getElementById('offset').value, 10);
    const limit = parseInt(document.getElementById('limit').value, 10);
    console.log("Current offset:", offset, "Current limit:", limit);

    const filterForm = document.getElementById('filter-form');
    const formData = new FormData(filterForm);
    formData.append('offset', offset);
    formData.append('limit', limit);

    const url = `/fetch_data?table_id=${encodeURIComponent(tableId)}`;
    console.log("Fetching data from URL:", url);

    try {
        const response = await fetch(url, {
            method: 'POST',
            body: formData
        });
        console.log("Response status:", response.status);

        if (!response.ok) {
            throw new Error(`HTTP error! Status: ${response.status}`);
        }

        const data = await response.json();
        console.log("Data received:", data);

        if (data.error) {
            alert(data.error);
            console.error("Error in data response:", data.error);
            return;
        }

        document.getElementById('offset').value = data.offset;
        document.getElementById('limit').value = data.limit;
        document.getElementById('totalCount').value = data.total_count;

        populateTable(data.rows);

        const statusSpan = document.getElementById('statusSpan');
        statusSpan.textContent =
            `Showing results ${data.offset + 1} to ${Math.min(data.offset + data.limit, data.total_count)} of ${data.total_count}`;

        document.getElementById('prevBtn').disabled = (data.offset <= 0);
        document.getElementById('nextBtn').disabled = (data.offset + data.limit >= data.total_count);
    } catch (error) {
        console.error('Error fetching data:', error);
        alert('Error fetching data. Check console for details.');
    }
}
