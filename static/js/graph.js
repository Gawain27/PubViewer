// ======================================================
// Configurations
// ======================================================
var concatenatedIds = "";

const FORCE_SETTINGS = {
    linkStrength: 2,
    chargeStrength: -750,
    collideRadius: 24,
    collideStrength: 0.4,
    linkDistanceScale: 1.5,
    zoomStep: 1.1,
    simulationMaxRuntime: 5000
};

const LINK_WIDTH_SCALE = d3.scaleLinear()
    .domain([0, 20])   // domain (up to 20 publications for max thickness)
    .range([1, 8])    // min thickness 1, max thickness 12
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
        case "A*": return "#1E90FF";     // Dodger Blue
        case "A":  return "#228B22";     // Forest Green
        case "B":  return "#FFD700";     // Gold
        case "C":  return "#FF8C00";     // Dark Orange
        default:   return "#FF4500";     // Orange Red
    }
}

function getJournColor(rank) {
    switch (rank.trim().toUpperCase()) {
        case "Q1": return "#1E90FF";     // Dodger Blue
        case "Q2": return "#228B22";     // Forest Green
        case "Q3": return "#FFD700";     // Gold
        case "Q4": return "#FF8C00";     // Dark Orange
        default:   return "#FF4500";     // Orange Red
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
function getLinkColor(link, selectedConfRank, selectedJournRank, isLinkFilter) {
    if (selectedConfRank && selectedJournRank && isLinkFilter) {
        const cColor = getConfColor(selectedConfRank);
        const jColor = getJournColor(selectedJournRank);
        return blendColors(cColor, jColor);
    } else if (selectedConfRank && isLinkFilter) {
        return getConfColor(selectedConfRank);
    } else if (selectedJournRank && isLinkFilter) {
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
            return "#FF4500";
        }
    }
}

// ======================================================
// Updates the dropdown labels with graph nodes
// ======================================================

function updateNodeDropdown(selectedNodeId) {
  console.log("Starting dropdown update...");

  const nodeLabelDropdown = master_document.getElementById("node-label");
  if (!nodeLabelDropdown) {
    console.error("Node label dropdown element not found!");
    return;
  }

  // Convert the dropdown’s current options to an Array for easy searching
  const existingOptions = Array.from(nodeLabelDropdown.options);
  console.log("Existing options:", existingOptions.map(opt => ({ value: opt.value, text: opt.text })));

  // Add options that do not already exist in the dropdown
  graphData.nodes.forEach(({ id, label }) => {
    const alreadyExists = existingOptions.some(opt => opt.value === id);
    if (!alreadyExists) {
      console.log(`Adding new option: id=${id}, label=${label}`);
      const newOption = master_document.createElement("option");
      newOption.value = id;
      newOption.textContent = label;
      nodeLabelDropdown.appendChild(newOption);
    }
  });

  // Set the selected option to the provided selectedNodeId if it exists
  const matchingOption = Array.from(nodeLabelDropdown.options).find(opt => opt.value === selectedNodeId);
  if (matchingOption) {
    console.log(`Found matching option for selectedNodeId=${selectedNodeId}, selecting it.`);
    nodeLabelDropdown.value = selectedNodeId;
  } else {
    console.warn(`No matching option found for selectedNodeId=${selectedNodeId}, leaving selection unchanged.`);
  }

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

    let selectedIds = concatenatedIds.split(",");
    if (selectedIds.length > 0) {
        console.log("Building adjacency list...");

        // Build adjacency list from linkData
        const adjacencyMap = new Map();
        graphData.nodes.forEach((n) => adjacencyMap.set(String(n.id), []));
        graphData.links.forEach((link) => {
            const s = String(typeof link.source === "object" ? link.source.id : link.source);
            const t = String(typeof link.target === "object" ? link.target.id : link.target);
            if (!adjacencyMap.has(s)) adjacencyMap.set(s, []);
            if (!adjacencyMap.has(t)) adjacencyMap.set(t, []);
            adjacencyMap.get(s).push(t);
            adjacencyMap.get(t).push(s);
        });

        console.log("Adjacency list built:", adjacencyMap);

        // Set to store all reachable nodes from all selectedIds
        const totalVisited = new Set();

        selectedIds.forEach((selectedId) => {
            console.log(`Processing selectedId: ${selectedId}`);
            if (selectedId) {
                const visited = new Set();
                const queue = [String(selectedId)];
                visited.add(String(selectedId));

                // BFS or DFS to gather all reachable nodes for this selectedId
                while (queue.length > 0) {
                    const currentId = queue.shift();
                    console.log(`Visiting node: ${currentId}`);
                    const neighbors = adjacencyMap.get(currentId) || [];
                    console.log(`Neighbors of ${currentId}:`, neighbors);
                    neighbors.forEach((neighborId) => {
                        if (!visited.has(neighborId)) {
                            visited.add(neighborId);
                            queue.push(neighborId);
                        }
                    });
                }

                console.log(`Visited nodes for selectedId ${selectedId}:`, visited);
                // Merge the visited nodes into the total visited set
                visited.forEach((node) => totalVisited.add(node));
            }
        });

        console.log("Total visited nodes:", totalVisited);

        // Filter nodeData and linkData based on the totalVisited set
        graphData.nodes = graphData.nodes.filter((n) => totalVisited.has(String(n.id)));
        graphData.links = graphData.links.filter((link) => {
            const sourceId = String(typeof link.source === "object" ? link.source.id : link.source);
            const targetId = String(typeof link.target === "object" ? link.target.id : link.target);
            return totalVisited.has(sourceId) && totalVisited.has(targetId);
        });
    } else {
        console.log("NO SELECTED ID");
    }

    console.log("Merge complete. Updated graph data:", graphData);
}

// ======================================================
// Calculates pub_count for each link based on the new rules:
// ======================================================
function updatePubCount(conferenceRank, journalRank, fromYear, toYear) {
    const hasConferenceFilter = conferenceRank && conferenceRank.trim() !== "";
    const hasJournalFilter = journalRank && journalRank.trim() !== "";
    const userHasRankFilter = hasConferenceFilter || hasJournalFilter;

    const fromYearNum = fromYear ? parseInt(fromYear, 10) : NaN;
    const toYearNum = toYear ? parseInt(toYear, 10) : NaN;
    const userHasYearFilter = (!isNaN(fromYearNum) || !isNaN(toYearNum));

    // Get the current year to enforce the upper limit
    const currentYear = new Date().getFullYear();

    // Helper to check if a property is within the valid range
    function isWithinYearRange(propKey) {
        const year = parseInt(propKey, 10);
        if (isNaN(year)) return false; // Not a numeric property
        if (year < 1950 || year > currentYear) return false; // Exclude out-of-range years
        if (!isNaN(fromYearNum) && year < fromYearNum) return false;
        if (!isNaN(toYearNum) && year > toYearNum) return false;
        return true;
    }

    graphData.links.forEach((link) => {
        let yearSumAll = 0;
        let yearSumInRange = 0;
        let rankSum = 0;

        Object.keys(link).forEach((propKey) => {
            if (propKey === "source" || propKey === "target") return;

            if (!isNaN(parseInt(propKey, 10))) {
                const val = link[propKey];
                if (typeof val === "number" && propKey >= "1950" && propKey <= currentYear.toString()) {
                    yearSumAll += val;

                    if (userHasYearFilter && isWithinYearRange(propKey)) {
                        yearSumInRange += val;
                    }
                }
            } else {
                if (userHasRankFilter) {
                    if (hasConferenceFilter && propKey === conferenceRank) {
                        const val = link[propKey];
                        if (typeof val === "number") rankSum += val;
                    }
                    if (hasJournalFilter && propKey === journalRank) {
                        const val = link[propKey];
                        if (typeof val === "number") rankSum += val;
                    }
                }
            }
        });

        if (!userHasYearFilter) {
            yearSumInRange = yearSumAll;
        }

        let pubCount;
        if (!userHasRankFilter && !userHasYearFilter) {
            pubCount = yearSumAll;
        } else if (userHasRankFilter && !userHasYearFilter) {
            pubCount = Math.round(rankSum / 1.5);
            pubCount = Math.min(pubCount, yearSumAll);
        } else if (!userHasRankFilter && userHasYearFilter) {
            pubCount = yearSumInRange;
        } else {
            pubCount = Math.round((rankSum + yearSumInRange) / 2);
        }

        link.pub_count = pubCount;
    });

    console.log("New pub_count updated for links:", graphData.links);
}

function renderGraph(conferenceRank, journalRank) {
    const svg = d3.select("svg");
    // Remove any existing elements before redrawing
    svg.selectAll("*").remove();

    const svgElement = document.querySelector('svg');
    const rect = svgElement.getBoundingClientRect();
    const width = rect.width;
    const height = rect.height;

    // ----------------------------------------------------------------------
    // 1. Build the link data and node data (same logic as before)
    // ----------------------------------------------------------------------
    let linkData = graphData.links.filter((l) => l.pub_count && l.pub_count > 0);
    let nodeData = graphData.nodes.filter((n) => true);

    const linksCheckbox = document.getElementById('links-checkbox');

    if (linksCheckbox.checked) {
        if (conferenceRank && conferenceRank.trim() !== "") {
            linkData = linkData.filter((l) => typeof l[conferenceRank] === "number" && l[conferenceRank] > 0);
        }
        if (journalRank && journalRank.trim() !== "") {
            linkData = linkData.filter((l) => typeof l[journalRank] === "number" && l[journalRank] > 0);
        }
    } else {
        if (conferenceRank && conferenceRank.trim() !== "") {
            nodeData = nodeData.filter((n) => n['freq_conf_rank'] === conferenceRank);
        }
        if (journalRank && journalRank.trim() !== "") {
            nodeData = nodeData.filter((n) => n['freq_journal_rank'] === journalRank);
        }
    }

    // Keep only links that connect nodes in the filtered node set
    const nodeIds = new Set(nodeData.map((node) => node.id));
    linkData = linkData.filter((link) => {
        const sourceId = typeof link.source === "object" ? link.source.id : link.source;
        const targetId = typeof link.target === "object" ? link.target.id : link.target;
        return nodeIds.has(sourceId) && nodeIds.has(targetId);
    });

    // Filter out nodes that are not linked
    const linkedNodeIds = new Set();
    linkData.forEach((link) => {
        const sourceId = typeof link.source === "object" ? link.source.id : link.source;
        const targetId = typeof link.target === "object" ? link.target.id : link.target;
        linkedNodeIds.add(sourceId);
        linkedNodeIds.add(targetId);
    });
    const filteredNodes = nodeData.filter((node) => linkedNodeIds.has(node.id));

    console.log("Nodes: " + filteredNodes.length + " - " + linkData.length);
    // ----------------------------------------------------------------------
    // If we have no nodes or no links at this point, alert & stop.
    // ----------------------------------------------------------------------
    if ((filteredNodes.length === 0 || linkData.length === 0)
        && (graphData.nodes.length !== 0 && graphData.links.length !== 0)) {
        alert("No result found for selected filters");
        return; // Stop rendering
    }

    // ----------------------------------------------------------------------
    // 2. Create a zoom layer and attach zoom behavior
    // ----------------------------------------------------------------------
    var zoomLayer = svg.append("g");
    zoomBehavior = d3.zoom()
        .scaleExtent([0.05, 10])
        .on("zoom", (event) => {
            zoomLayer.attr("transform", event.transform);
        });
    svg.call(zoomBehavior);

    // ----------------------------------------------------------------------
    // 3. Create D3 force simulation
    // ----------------------------------------------------------------------
    const simulation = d3.forceSimulation(filteredNodes)
      .force("link", d3.forceLink(linkData).id(d => d.id).distance(100).strength(1))
      .force("charge", d3.forceManyBody().strength(-1000))
      .force("center", d3.forceCenter(width / 2, height / 2))
      .force("collide", d3.forceCollide().radius(FORCE_SETTINGS.collideRadius).strength(FORCE_SETTINGS.collideStrength))
      .force("x", d3.forceX())
      .force("y", d3.forceY());

    simulation.on("end", () => {
         simulation
           .force("link", null)
           .force("charge", null)
           .force("center", null)
           .force("collide", null)
           .force("x", null)
           .force("y", null);
    });

    // ----------------------------------------------------------------------
    // 4. Define drag behavior
    // ----------------------------------------------------------------------
    const drag = simulation => {

        function dragstarted(event, d) {
            if (!event.active) simulation.alphaTarget(0.3).restart();
            d.fx = d.x;
            d.fy = d.y;
        }

        function dragged(event, d) {
            d.fx = event.x;
            d.fy = event.y;
        }

        function dragended(event, d) {
            if (!event.active) simulation.alphaTarget(0);
            d.fx = null;
            d.fy = null;
        }

        return d3.drag()
            .on("start", dragstarted)
            .on("drag", dragged)
            .on("end", dragended);
    }

    // ----------------------------------------------------------------------
    // 5. Append links
    // ----------------------------------------------------------------------
    const link = zoomLayer.append("g")
      .selectAll("line")
      .data(linkData)
      .join("line")
      .attr("stroke", (d) => getLinkColor(d, conferenceRank, journalRank, linksCheckbox.checked))
      .attr("stroke-width", (d) => LINK_WIDTH_SCALE(d.pub_count));

    // ----------------------------------------------------------------------
    // 6. Append nodes as groups, so we can insert both circle & image
    // ----------------------------------------------------------------------
    const node = zoomLayer.selectAll("g.node")
        .data(filteredNodes)
        .join("g")
        .attr("class", "node")
        .call(drag(simulation))
        .on("contextmenu", (event, d) => {
            event.preventDefault();
            showNodePopup(d, event.pageX, event.pageY);
        });

    // Define a unique clipPath for each node
    node.append("clipPath")
        .attr("id", d => `clip-circle-${d.id}`)
        .append("circle")
        .attr("r", 16);

    // A circle (visible outline)
    node.append("circle")
        .attr("r", 16)
        .attr("fill", "white")
        .attr("stroke", "#000")
        .attr("stroke-width", 1.5);

    // The image, clipped to the circle
    node.append("svg:image")
        .attr("xlink:href", d => d.image || "")
        .attr("width", 32)
        .attr("height", 32)
        .attr("x", -16)
        .attr("y", -16)
        .attr("clip-path", d => `url(#clip-circle-${d.id})`)
        .on("error", function () {
            d3.select(this).attr("xlink:href", "/static/resource/avatar.png");
        });

    // The label below the node
    node.append("text")
        .attr("x", 0)
        .attr("y", 24)  // just below the circle
        .attr("text-anchor", "middle")
        .attr("alignment-baseline", "hanging")
        .style("font-size", "8px")
        .text(d => d.label || "");

    // ----------------------------------------------------------------------
    // 7. On every tick, update positions
    // ----------------------------------------------------------------------
    simulation.on("tick", () => {
        link
          .attr("x1", d => d.source.x)
          .attr("y1", d => d.source.y)
          .attr("x2", d => d.target.x)
          .attr("y2", d => d.target.y);

        // Move the entire node group
        node
          .attr("transform", d => `translate(${d.x}, ${d.y})`);
    });
}


// ======================================================
// Show Node Popup on right-click
// ======================================================
function showNodePopup(nodeData, x, y) {
    const popup = document.getElementById("node-popup");
    const popupImage = document.getElementById("popup-image");

    popupImage.src = nodeData.image.toString().startsWith("http") ? nodeData.image : "/static/resource/avatar.png";

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
            addPopupRow(tableBody, "Freq. Conference Rank", author_data["avg_conference_rank"])
            addPopupRow(tableBody, "Freq. Journal Rank", author_data["avg_journal_rank"])

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
function fetchGraphData(selectedNodeId, depth, conferenceRank, journalRank, fromYear, toYear, loadingPopup, timerInterval, render = true, init = false){
    let last_selected;
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
            let split_ids = selectedNodeId.split(',');
            if (init === true){
                for (let id in split_ids){
                    updateNodeDropdown(id);
                    last_selected = id;
                }
            }
            if (render === true) {
                setTimeout(async () => {
                    renderGraph(conferenceRank, journalRank);
                }, 1000);
            }

            prev_id = last_selected;
            prev_depth = depth;
        })
        .catch((error) => console.error("Error during graph generation:", error))
        .finally(() => {
            if (loadingPopup != null){
                loadingPopup.style.display = "none";
                clearInterval(timerInterval); // Stop the timer
            }
            if (init){
                const loadingPopup = document.getElementById("loading-popup");
                loadingPopup.style.display = "none";
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

    let list = concatenatedIds.split(",");

    if (!list.includes(selectedNodeId.toString())) {
        list.push(selectedNodeId.toString());
    }

    concatenatedIds = list.join(",");

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
        setTimeout(async () => {
            renderGraph(conferenceRank, journalRank);
        }, 1000);
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

document.getElementById("clear_graph").addEventListener("click", function (){
    graphData.links.length = 0;
    graphData.nodes.length = 0;
    const svg = d3.select("svg");
    svg.selectAll("*").remove();
    setTimeout(
        async () => {
            graphData.links.length = 0;
            graphData.nodes.length = 0;
            const svg = d3.select("svg");
            svg.selectAll("*").remove();
        }, 1100
    )
});

document.addEventListener("DOMContentLoaded", () => {
    const checkbox = document.getElementById("links-checkbox");
    const label = document.getElementById("filter-label");

    checkbox.addEventListener("change", () => {
        if (checkbox.checked) {
            label.textContent = "Filter Ranks By: Links";
        } else {
            label.textContent = "Filter Ranks By: Nodes";
        }
    });
});

// ======================================================
// Initialize dropdown and basic graph on page load
// ======================================================
function initGraph(timerInterval, init_ids) {
    concatenatedIds = init_ids
    try {
        // Make a single request with all option values concatenated
        fetchGraphData(init_ids, 1, "", "", "", "", null, null, false);
        console.log("Graph initialization request sent for all options.");
    } catch (error) {
        console.error("Error during graph initialization:", error);
    } finally {
        // generate graph data for the last id, just for rendering, it won't get generated twice and query is small

        const lastOptionValue = init_ids.split(',').slice(-1)[0]
        setTimeout(async () => {
            fetchGraphData(lastOptionValue, 1, null, null, null, null, null, null, true, true);
            clearInterval(timerInterval)
        }, 500);
    }
}


window.onload = function () {
    console.log("Initializing dropdown on page load...");

    let init_ids = startIds.join(',')
    init_ids = init_ids.replaceAll("(", "").replaceAll(")", "");

    $(document).ready(function() {
        $('#node-label').select2({
            placeholder: 'Select a label...',
            allowClear: true
        });
    });

    setTimeout(async () => {
        const loadingTimeSpan = document.getElementById("loading-time");
        const loadingPopup = document.getElementById("loading-popup");
        if (!loadingPopup) {
            console.error("loadingPopup not found!");
        }

        // Show the loading popup and start the timer
        loadingPopup.style.display = "block";
        let elapsedTime = 0;
        loadingTimeSpan.textContent = elapsedTime.toString();
        let timerInterval = setInterval(() => {
            elapsedTime++;
            loadingTimeSpan.textContent = elapsedTime.toString();
        }, 1000);

        initGraph(timerInterval, init_ids);
        console.log("Graph initialization script has finished.");
    }, 500);
};






