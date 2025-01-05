// ======================================================
// Configurations
// ======================================================
const GRAPH_DIMENSIONS = { width: 1000, height: 600 };

const FORCE_SETTINGS = {
    linkStrength: 1,
    chargeStrength: -300,
    collideRadius: 60,
    collideStrength: 1,
    linkDistanceScale: 7,
    zoomStep: 1.1,
    simulationMaxRuntime: 5000
};

const LINK_WIDTH_SCALE = d3.scaleLinear()
    .domain([0, 50])   // domain (up to 50 publications for max thickness)
    .range([1, 12])    // min thickness 1, max thickness 8
    .clamp(true);      // do not exceed the range

// ======================================================
// Global data
// ======================================================
let graphData = { nodes: [], links: [] };
let prev_id = 0;
let prev_depth = 0;
let zoomBehavior;

// ======================================================
// Utility: Color logic for ranks
// ======================================================
function getConfColor(rank) {
    switch (rank.trim().toUpperCase()) {
        case "A*": return "#008000";     // Green
        case "A":  return "#9ACD32";     // Yellow-Green
        case "B":  return "#FFFF00";     // Yellow
        case "C":  return "#FFA500";     // Orange
        default:   return "#FF0000";     // Red
    }
}

function getJournColor(rank) {
    switch (rank.trim().toUpperCase()) {
        case "Q1": return "#008000";     // Green
        case "Q2": return "#9ACD32";     // Yellow-Green
        case "Q3": return "#FFFF00";     // Yellow
        case "Q4": return "#FFA500";     // Orange
        default:   return "#FF0000";     // Red
    }
}

/**
 * Linearly blend two hex colors by 50/50
 */
function blendColors(color1, color2) {
    const c1 = d3.color(color1).rgb();
    const c2 = d3.color(color2).rgb();

    // For a simple 50/50 blend, just use d3.interpolateRgb
    return d3.interpolateRgb(c1, c2)(0.5);
}

/**
 *  Get link color based on the user’s selected conf/journal rank filters
 *  If both are defined, blend. If only conf is defined, return conf color.
 *  If only journ is defined, return journ color. Otherwise red.
 */
function getLinkColor(link, selectedConfRank, selectedJournRank) {
    if (selectedConfRank && selectedJournRank) {
        const cColor = getConfColor(selectedConfRank);
        const jColor = getJournColor(selectedJournRank);
        return blendColors(cColor, jColor);
    } else if (selectedConfRank) {
        return getConfColor(selectedConfRank);
    } else if (selectedJournRank) {
        return getJournColor(selectedJournRank);
    } else {
        if (link.avg_conf_rank && link.avg_conf_rank !== "" && link.avg_journal_rank && link.avg_journal_rank !== ""){
            const cColor = getConfColor(link.avg_conf_rank);
            const jColor = getJournColor(link.avg_journal_rank);
            return blendColors(cColor, jColor);
        } else if (link["avg_conf_rank"] && link["avg_conf_rank"] !== ""){
            return getConfColor(link["avg_conf_rank"]);
        } else if (link["avg_journal_rank"] && link["avg_journal_rank"] !== ""){
            return getJournColor(link["avg_journal_rank"]);
        } else {
            return "#FF0000";
        }
    }
}

// ======================================================
// Updates the dropdown labels with graph nodes
// ======================================================
function sortOptions() {
    const select = master_document.getElementById('node-label');
    const options = Array.from(select.options);
    options.sort((a, b) => a.textContent.localeCompare(b.textContent));
    select.innerHTML = '';
    options.forEach(option => select.appendChild(option));
}

function updateNodeDropdown(selectedId) {
    console.log("Updating node dropdown...");
    const nodeLabelDropdown = master_document.getElementById("node-label");
    if (!nodeLabelDropdown) {
        console.error("Node label dropdown element not found!");
        return;
    }

    let defaultOption;
    if (selectedId == null) {
        defaultOption = nodeLabelDropdown.querySelector("option[selected]");
        console.log("Id selected is null, selecting default option");
    } else {
        const options = nodeLabelDropdown.options;
        let found = false;
        for (let i = 0; i < options.length; i++) {
            if (options[i].value === selectedId) {
                nodeLabelDropdown.selectedIndex = i;
                found = true;
                break;
            }
        }
        console.log("Found previously selected id: " + found);
        defaultOption = nodeLabelDropdown.options[nodeLabelDropdown.selectedIndex];
    }
    console.log("Default option retained:", defaultOption?.outerHTML);

    nodeLabelDropdown.innerHTML = defaultOption.outerHTML;

    graphData.nodes.forEach(({ id, label }) => {
        if (!nodeLabelDropdown.querySelector(`option[value="${id}"]`)) {
            const newOption = master_document.createElement("option");
            newOption.value = id;
            newOption.textContent = label;
            nodeLabelDropdown.appendChild(newOption);
        }
    });
    sortOptions()
    console.log("Dropdown update complete.");
}

// ======================================================
// Merges new nodes/links into existing graph data
// while avoiding duplication
// ======================================================
function mergeGraphData(newNodes, newLinks) {
    console.log("Merging new graph data...");
    console.log("Existing nodes:", graphData.nodes);
    console.log("Existing links:", graphData.links);
    console.log("New nodes:", newNodes);
    console.log("New links:", newLinks);

    const alreadyExists = (arr, key) => arr.some((item) => item.id === key);
    const linkExists = (link) =>
        graphData.links.some(
            (gLink) =>
                (gLink.source === link.source && gLink.target === link.target) ||
                (gLink.source === link.target && gLink.target === link.source)
        );

    // Add nodes
    newNodes.forEach((node) => {
        if (!alreadyExists(graphData.nodes, node.id)) {
            graphData.nodes.push(node);
        }
    });

    // Add links
    newLinks.forEach((link) => {
        if (!linkExists(link)) {
            graphData.links.push(link);
        }
    });

    console.log("Merge complete. Updated graph data:", graphData);
}

// ======================================================
// Calculates pub_count for each link based on the new rules:
// ======================================================
function updatePubCount(conferenceRank, journalRank, fromYear, toYear) {
    console.log("Updating pub_count for links with new rules...");
    console.log(`conferenceRank: ${conferenceRank}, journalRank: ${journalRank}, fromYear: ${fromYear}, toYear: ${toYear}`);

    // Determine if we have any rank filter
    const hasConferenceFilter = conferenceRank && conferenceRank.trim() !== "";
    const hasJournalFilter = journalRank && journalRank.trim() !== "";
    const userHasRankFilter = hasConferenceFilter || hasJournalFilter;

    // Determine if we have any year filter
    const fromYearNum = fromYear ? parseInt(fromYear, 10) : NaN;
    const toYearNum = toYear ? parseInt(toYear, 10) : NaN;
    const userHasYearFilter = (!isNaN(fromYearNum) || !isNaN(toYearNum));

    // Helper to check if a property is within the user’s fromYear–toYear range
    function isWithinYearRange(propKey) {
        const year = parseInt(propKey, 10);
        if (isNaN(year)) return false; // Not a numeric property
        if (!isNaN(fromYearNum) && year < fromYearNum) return false;
        return !(!isNaN(toYearNum) && year > toYearNum);
    }

    graphData.links.forEach((link) => {
        // 1) Calculate total year-based sum (all years, ignoring fromYear/toYear)
        let yearSumAll = 0;
        // 2) Calculate year-based sum in the range [fromYear, toYear]
        let yearSumInRange = 0;
        // 3) Calculate rank-based sum of the chosen conf/journal ranks
        let rankSum = 0;

        // -------------------------------------------------
        // Gather sums
        // -------------------------------------------------
        Object.keys(link).forEach((propKey) => {
            // Skip the typical source/target
            if (propKey === "source" || propKey === "target") return;

            // If it's a numeric year property
            if (!isNaN(parseInt(propKey, 10))) {
                const val = link[propKey];
                if (typeof val === "number") {
                    // Add to the "all years" sum
                    yearSumAll += val;

                    // If user has year filters, and it's in range,
                    // also add to yearSumInRange
                    if (userHasYearFilter && isWithinYearRange(propKey)) {
                        yearSumInRange += val;
                    }
                    // If user has no year filter, yearSumInRange
                    // is the same as yearSumAll => we'll handle after
                }
            }
            else {
                // It's a rank-based property (A*, A, B, Q1, Q2, etc.)
                // Add only if user’s filter matches it
                if (userHasRankFilter) {
                    // If user selected a conf rank that matches propKey
                    if (hasConferenceFilter && propKey === conferenceRank) {
                        const val = link[propKey];
                        if (typeof val === "number") rankSum += val;
                    }
                    // If user selected a journal rank that matches propKey
                    if (hasJournalFilter && propKey === journalRank) {
                        const val = link[propKey];
                        if (typeof val === "number") rankSum += val;
                    }
                }
            }
        });

        // If user has no year filter, we consider the entire sum for yearSumInRange
        if (!userHasYearFilter) {
            yearSumInRange = yearSumAll;
        }

        // If user DID NOT select any conf/journal rank,
        // then rankSum = 0 (we consider none).
        // Alternatively, if user didn't pick a rank at all,
        // that sum remains 0 from above logic.

        // -------------------------------------------------
        // Compute final pub_count based on the rules:
        // -------------------------------------------------
        let pubCount;

        // (A) No rank, no year => just sum all years
        if (!userHasRankFilter && !userHasYearFilter) {
            pubCount = yearSumAll;
        }
        // (B) Rank filter but no year filter
        else if (userHasRankFilter && !userHasYearFilter) {
            pubCount = Math.round(rankSum / 1.5);
            pubCount = Math.min(pubCount, yearSumAll);
        }
        // (C) No rank filter, but year filter => use only yearSumInRange
        else if (!userHasRankFilter && userHasYearFilter) {
            pubCount = yearSumInRange;
        }
        // (D) Rank filter AND year filter => consider everything, then /2
        else {
            pubCount = Math.round((rankSum + yearSumInRange) / 2);
        }

        link.pub_count = pubCount;
    });

    console.log("New pub_count updated for links:", graphData.links);
}


// ======================================================
// Renders the graph using D3.js
// ======================================================
function renderGraph(conferenceRank, journalRank) {
    console.log("Rendering graph...");
    const svg = d3.select("svg");
    svg.selectAll("*").remove();

    const zoomLayer = svg.append("g");
    console.log("Initialized zoom layer.");

    // ----------------------------------------------------------------------
    // 1. Determine dynamic link distance
    // ----------------------------------------------------------------------
    const totalNodes = graphData.nodes.length;
    const dynamicLinkDistance = 200 + totalNodes * FORCE_SETTINGS.linkDistanceScale;
    console.log("Calculated dynamic link distance:", dynamicLinkDistance);

    // ----------------------------------------------------------------------
    // 2. Create D3 simulation
    // ----------------------------------------------------------------------
    const simulation = d3
        .forceSimulation(graphData.nodes)
        .force(
            "link",
            d3.forceLink(graphData.links)
                .id((d) => d.id)
                .distance(dynamicLinkDistance)
                .strength(FORCE_SETTINGS.linkStrength)
        )
        .force("charge", d3.forceManyBody().strength(FORCE_SETTINGS.chargeStrength))
        .force("center", d3.forceCenter(GRAPH_DIMENSIONS.width / 2, GRAPH_DIMENSIONS.height / 2))
        .force("collide", d3.forceCollide().radius(FORCE_SETTINGS.collideRadius).strength(FORCE_SETTINGS.collideStrength))
        .alphaDecay(0.05);

    // ----------------------------------------------------------------------
    // 3. granular zoom
    // ----------------------------------------------------------------------
    zoomBehavior = d3.zoom()
        .scaleExtent([0.05, 10])
        .on("zoom", (event) => {
            zoomLayer.attr("transform", event.transform);
        });
    svg.call(zoomBehavior);

    // ----------------------------------------------------------------------
    // 4. Build the link data and link elements
    // ----------------------------------------------------------------------
    // Only render links with pub_count > 0
    let linkData = graphData.links.filter((l) => l.pub_count && l.pub_count > 0);

    // If the user wants to strictly filter out links that do NOT have the specified
    // conf/journal rank, we can do so here. This is optional depending on your needs.
    if (conferenceRank && conferenceRank.trim() !== "") {
        linkData = linkData.filter((l) => typeof l[conferenceRank] === "number" && l[conferenceRank] > 0);
    }
    if (journalRank && journalRank.trim() !== "") {
        linkData = linkData.filter((l) => typeof l[journalRank] === "number" && l[journalRank] > 0);
    }

    // By default hide unranked links if no filters are defined and graph size is big
    if (journalRank === "" && conferenceRank === "" && linkData.length >= 300){
        linkData = linkData.filter((l) => ["A*","A"].includes(l["avg_conf_rank"]) && ["Q1", "Q2"].includes(l["avg_journal_rank"]));
    }

    const link = zoomLayer
        .append("g")
        .selectAll("line")
        .data(linkData)
        .enter()
        .append("line")
        .attr("stroke", (d) => getLinkColor(d, conferenceRank, journalRank))
        .attr("stroke-width", (d) => LINK_WIDTH_SCALE(d.pub_count));

    console.log("Rendered links:", linkData);

    // ----------------------------------------------------------------------
    // 5. Build the node data and node elements
    // ----------------------------------------------------------------------
    const node = zoomLayer
        .append("g")
        .selectAll("g")
        .data(graphData.nodes)
        .enter()
        .append("g")
        .call(
            d3.drag()
                .on("start", dragStarted)
                .on("drag", dragged)
                .on("end", dragEnded)
        )
        // (3) Add a right-click contextmenu event to show the popup
        .on("contextmenu", (event, d) => {
            event.preventDefault();
            showNodePopup(d, event.pageX, event.pageY);
        });

    console.log("Rendered nodes:", graphData.nodes);

    // Circle mask for avatar
    node.append("clipPath")
        .attr("id", (d) => `clip-${d.id}`)
        .append("circle")
        .attr("r", 50);

    node.append("circle")
        .attr("r", 50)
        .attr("fill", "white")
        .attr("stroke", "#999")
        .attr("stroke-width", 2);

    node.append("image")
        .attr("xlink:href", (d) => d.image)
        .attr("width", 100)
        .attr("height", 100)
        .attr("x", -50)
        .attr("y", -50)
        .attr("clip-path", (d) => `url(#clip-${d.id})`)
        .on("error", function () {
            d3.select(this).attr("xlink:href", "/static/resource/avatar.png");
        });

    node.append("text")
        .attr("font-size", "16px")
        .attr("fill", "#000")
        .attr("text-anchor", "middle")
        .attr("dy", 70)
        .text((d) => d.label);

    // ----------------------------------------------------------------------
    // 6. Attach "tick" handler (we simply translate nodes)
    // ----------------------------------------------------------------------
    simulation.on("tick", () => {
        link
            .attr("x1", (d) => d.source.x)
            .attr("y1", (d) => d.source.y)
            .attr("x2", (d) => d.target.x)
            .attr("y2", (d) => d.target.y);

        node.attr("transform", (d) => `translate(${d.x},${d.y})`);
    });

    // ----------------------------------------------------------------------
    // 7. Disable physics once the simulation stabilizes
    // ----------------------------------------------------------------------
    setTimeout(() => {
        console.log(`Stopping simulation after ${FORCE_SETTINGS.simulationMaxRuntime} ms...`);
        simulation.force("link", null)
            .force("charge", null)
            .force("center", null)
            .force("collide", null);
    }, FORCE_SETTINGS.simulationMaxRuntime);

    // ----------------------------------------------------------------------
    // Draggable node events
    // ----------------------------------------------------------------------
    function dragStarted(event, d) {
        console.log("Drag started for node:", d);
        if (!event.active) simulation.alphaTarget(0.3).restart();
        d.fx = d.x;
        d.fy = d.y;
    }

    function dragged(event, d) {
        // Fix position to the current mouse coordinates
        d.fx = event.x;
        d.fy = event.y;
    }

    function dragEnded(event, d) {
        console.log("Drag ended for node:", d);
        if (!event.active) simulation.alphaTarget(0);
        d.fx = null;
        d.fy = null;
    }
}

// ======================================================
// Show Node Popup on right-click
// ======================================================
function showNodePopup(nodeData, x, y) {
    const popup = document.getElementById("node-popup");
    const popupImage = document.getElementById("popup-image");

    popupImage.src = nodeData.image || "";

    const tableBody = document.getElementById('popup-table-body');
    tableBody.innerHTML = '';

    const loadingPopup = document.getElementById("loading-popup");
    const loadingTimeSpan = document.getElementById("loading-time");

    let elapsedTime = 0;
    let timerInterval;

    loadingPopup.style.display = "block";
    elapsedTime = 0;
    loadingTimeSpan.textContent = elapsedTime.toString();
    timerInterval = setInterval(() => {
        elapsedTime++;
        loadingTimeSpan.textContent = elapsedTime.toString();
    }, 1000);


    fetch("/fetch-author-detail", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
            author_id: nodeData.id
        }),
    })
        .then((response) => response.json())
        .then(({author_data}) => {
            addPopupRow(tableBody, "Author ID", nodeData.id)
            addPopupRow(tableBody, "Name", nodeData.label)
            addPopupRow(tableBody, "Organization", author_data["Organization"])
            addPopupRow(tableBody, "H-Index", author_data["hIndex"])
            addPopupRow(tableBody, "I-10 Index", author_data["i10Index"])
            addPopupRow(tableBody, "Cited by", author_data["citesTotal"])
            addPopupRow(tableBody, "Publications Found", author_data["pubTotal"])
            addPopupRow(tableBody, "Avg. Conference Rank", author_data["avg_conference_rank"])
            addPopupRow(tableBody, "Avg. Journal Rank", author_data["avg_journal_rank"])

            console.log("details for: " + nodeData.label + " - " + author_data)
            popup.style.display = "block";
            resizePopup(popup, true);
        })
        .catch((error) => console.error("Error during graph generation:", error))
        .finally(() => {
            if (loadingPopup != null){
                loadingPopup.style.display = "none";
                clearInterval(timerInterval); // Stop the timer
            }
        });
}

// Function to adjust the popup and table size dynamically
function resizePopup(popup, recall) {
    const popupTable = popup.querySelector('.popup-table');

    if (popupTable) {
        // Force layout recalculation
        popupTable.style.width = 'auto'; // Reset to auto to fit contents
        const computedWidth = popupTable.offsetWidth; // Get natural size
        popupTable.style.width = `${computedWidth}px`; // Apply computed size
    }

    popup.style.width = 'auto'; // Reset to auto to fit contents
    const computedPopupWidth = popup.offsetWidth;
    popup.style.width = `${computedPopupWidth}px`;
    if (recall){
        resizePopup(popup, false);
    }
}

// Function to populate the popup table with data
function addPopupRow(tableBody, key, value) {
    const row = document.createElement('tr');

    const propertyCell = document.createElement('td');
    propertyCell.textContent = key;
    const valueCell = document.createElement('td');
    valueCell.textContent = value;

    row.appendChild(propertyCell);
    row.appendChild(valueCell);
    tableBody.appendChild(row);
}

function closeNodePopup() {
    const popup = document.getElementById("node-popup");
    popup.style.display = "none";
}

// ======================================================
// Add event listeners for + / - buttons
// ======================================================
document.getElementById("zoom-in-btn").addEventListener("click", () => {
    const svg = d3.select("svg");
    const transform = d3.zoomTransform(svg.node());
    const newScale = transform.k * FORCE_SETTINGS.zoomStep;
    svg.transition().duration(300).call(zoomBehavior.scaleTo, newScale);
});

document.getElementById("zoom-out-btn").addEventListener("click", () => {
    const svg = d3.select("svg");
    const transform = d3.zoomTransform(svg.node());
    const newScale = transform.k / FORCE_SETTINGS.zoomStep;
    svg.transition().duration(300).call(zoomBehavior.scaleTo, newScale);
});

// ======================================================
// Handles Graph API call
// ======================================================
function fetchGraphData(selectedNodeId, depth, conferenceRank, journalRank, fromYear, toYear, loadingPopup, timerInterval, render = true){
    fetch("/generate-graph", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
            start_author_id: selectedNodeId,
            depth: depth,
            conference_rank: conferenceRank,
            journal_rank: journalRank,
            from_year: fromYear,
            to_year: toYear,
        }),
    })
        .then((response) => response.json())
        .then(({ nodes, links }) => {
            console.log("API response received:", { nodes, links });
            mergeGraphData(nodes, links);
            updatePubCount(conferenceRank, journalRank, fromYear, toYear);
            updateNodeDropdown(selectedNodeId);
            if (render === true) {
                renderGraph(conferenceRank, journalRank);
            }

            prev_id = selectedNodeId;
            prev_depth = depth;
        })
        .catch((error) => console.error("Error during graph generation:", error))
        .finally(() => {
            if (loadingPopup != null){
                loadingPopup.style.display = "none";
                clearInterval(timerInterval); // Stop the timer
            }
        });
}

// ======================================================
// Handles form submission for graph generation
// ======================================================
document.getElementById("graph-form").addEventListener("submit", function (event) {
    event.preventDefault();
    console.log("Form submission intercepted.");

    const formData = new FormData(event.target);
    const selectedNodeId = formData.get("node_label");
    const depth = formData.get("depth");
    const conferenceRank = formData.get("conference_rank");
    const journalRank = formData.get("journal_rank");
    const fromYear = formData.get("from_year");
    const toYear = formData.get("to_year");

    console.log("Form data extracted:", {
        selectedNodeId,
        depth,
        conferenceRank,
        journalRank,
        fromYear,
        toYear
    });

    const loadingPopup = document.getElementById("loading-popup");
    const loadingTimeSpan = document.getElementById("loading-time");

    let elapsedTime = 0;
    let timerInterval;

    if (prev_id === selectedNodeId && prev_depth === depth) {
        console.log("Skipping API call as prev_id and prev_depth match selectedNodeId and depth.");
        updatePubCount(conferenceRank, journalRank, fromYear, toYear);
        renderGraph(conferenceRank, journalRank);
    } else {
        console.log("Making API call as prev_id and prev_depth are different.");

        // Show the loading popup and start the timer
        loadingPopup.style.display = "block";
        elapsedTime = 0;
        loadingTimeSpan.textContent = elapsedTime.toString();
        timerInterval = setInterval(() => {
            elapsedTime++;
            loadingTimeSpan.textContent = elapsedTime.toString();
        }, 1000);

        fetchGraphData(selectedNodeId, depth, conferenceRank, journalRank, fromYear, toYear, loadingPopup, timerInterval);
    }
});

// ======================================================
// Initialize dropdown and basic graph on page load
// ======================================================
async function initGraph() {
    const nodeLabelDropdown = master_document.getElementById("node-label");
    const loadingPopup = document.getElementById("loading-popup");
    const loadingTimeSpan = document.getElementById("loading-time");

    console.log("body: " + master_document.body.innerHTML);
    let elapsedTime = 0;
    let timerInterval;

    if (!nodeLabelDropdown) {
        console.error("Node label dropdown element not found!");
    } else {
        console.log("Initializing graph for all options in dropdown...");

        // Show the loading popup and start the timer
        loadingPopup.style.display = "block";
        elapsedTime = 0;
        loadingTimeSpan.textContent = elapsedTime.toString();
        timerInterval = setInterval(() => {
            elapsedTime++;
            loadingTimeSpan.textContent = elapsedTime.toString();
        }, 1000);

        const options = Array.from(nodeLabelDropdown.options);
        var last_exec = null;
        const promises = options.map(option => {
                fetchGraphData(option.value, 1, "", "", "", "", null, null, false);
                last_exec = option.value;
            }
        );

        try {
            await Promise.all(promises);
            console.log("Graph initialization complete for all options.");
        } catch (error) {
            console.error("Error during graph initialization:", error);
        } finally {
            // Hide the loading popup and clear the timer
            clearInterval(timerInterval);
            loadingPopup.style.display = "none";
            fetchGraphData(last_exec, 1, null, null, null, null, null, null)
        }
    }
}

window.addEventListener("load", function () {
    console.log("Initializing dropdown on page load...");
    const urlParams = new URLSearchParams(window.location.search);

    const valueParam = urlParams.get('value');

    const startIds = valueParam ? valueParam.split(',') : [];

    const nodeLabelDropdown = master_document.getElementById('node-label');
    for (let i = 0; i < startIds.length; i++){
        if (!nodeLabelDropdown.querySelector(`option[value="${startIds[i]}"]`)) {
            const newOption = master_document.createElement("option");
            newOption.value = startIds[i];
            newOption.textContent = startLabels[i];
            nodeLabelDropdown.appendChild(newOption);
            console.log("added: " + startLabels[i])
        }
    }

    sortOptions();

    // Turn the <select> into a Select2 dropdown
    // If you’re in a multi-frame environment, you might need to target `$(master_document).find('#node-label')`.
    // But for simplicity, let’s assume standard usage:
    $(document).ready(function() {
        $('#node-label').select2({
            placeholder: 'Select a label...',
            allowClear: true
        });
    });

    (async () => {
        await initGraph();
        console.log("Graph initialization script has finished.");
    })();
});






