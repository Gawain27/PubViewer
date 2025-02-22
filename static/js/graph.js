// ======================================================
// Configurations
// ======================================================
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
let graphData = { nodes: [], links: [], semi_weak_links: [], weak_links: []};
let prev_id = 0;
let prev_depth = 0;
let prevConfRank = "";
let prevJournalRank = "";
let currentlySelected = "";
let zoomBehavior;
var showAll = true;

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

function blendColors(color1, color2) {
    const colorOrder = ["#1E90FF", "#228B22", "#FFD700", "#FF8C00", "#FF4500"];

    // Find the indices of the colors in the predefined order
    const index1 = colorOrder.indexOf(color1);
    const index2 = colorOrder.indexOf(color2);

    // Determine highest and lowest color based on the order
    const [highColor, lowColor] = index1 < index2
        ? [color1, color2]
        : [color2, color1];

    // Generate 9 evenly spaced midpoints using d3.interpolateRgb
    const midpoints = Array.from({ length: 9 }, (_, i) =>
        d3.interpolateRgb(d3.color(highColor), d3.color(lowColor))((i + 1) / 10)
    );

    // Return the third midpoint (index 2, zero-based), giving the color of higher interest
    return midpoints[2];
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

function updateNodeDropdown() {
  console.log("Starting dropdown update...");

  const nodeLabelDropdown = master_document.getElementById("node-label");
  if (!nodeLabelDropdown) {
    console.error("Node label dropdown element not found!");
    return;
  }

  // Convert the dropdown’s current options to an Array for easy searching
  const existingOptions = $('#node-label option').toArray();

  // Add options that do not already exist in the dropdown
  graphData.nodes.forEach(({ id, label }) => {
    let alreadyExists = false;
    existingOptions.forEach(option => {
        if (option.value === id.toString()){
            alreadyExists = true;
        }
    });
    if (!alreadyExists) {
      console.log(`Adding new option: id=${id}, label=${label}`);
      const newOption = master_document.createElement("option");
      newOption.value = id;
      newOption.textContent = "  " + label;
      nodeLabelDropdown.appendChild(newOption);
    }
  });
/*
  // Remove duplicate options
  const seenIds = new Set();
  Array.from(nodeLabelDropdown.options).forEach(option => {
    if (seenIds.has(option.value)) {
      console.log(`Removing duplicate option with id=${option.value}`);
      nodeLabelDropdown.removeChild(option);
    } else {
      seenIds.add(option.value);
    }
  });
*/
  console.log("Dropdown update complete.");
}

// ======================================================
// Merges new nodes/links into existing graph data
// while avoiding duplication
// ======================================================
function mergeGraphData(newNodes, newLinks, newSemiWeakLinks, weakNewLinks) {
    console.log("Merging new graph data...");
    console.log("New nodes:", newNodes);
    console.log("New links:", newLinks);
    console.log("New semi-weak links:", newSemiWeakLinks);
    console.log("New weak links:", weakNewLinks);

    const alreadyExists = (arr, key) => arr.some((item) => item.id === key);
    const linkExists = (link) =>
        graphData.links.some(
            (gLink) =>
                (gLink.source === link.source && gLink.target === link.target) ||
                (gLink.source === link.target && gLink.target === link.source)
        );
    const weakLinkExists = (link) =>
        graphData.weak_links.some(
            (gLink) =>
                (gLink.source === link.source && gLink.target === link.target) ||
                (gLink.source === link.target && gLink.target === link.source)
        );
    const semiWeakLinkExists = (link) =>
        graphData.semi_weak_links.some(
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

    newSemiWeakLinks.forEach((link) => {
        if (!semiWeakLinkExists(link)) {
            const sourceNode = graphData.nodes.find((node) => node.id === link.source);
            const targetNode = graphData.nodes.find((node) => node.id === link.target);
            if (sourceNode && targetNode) {
                // Replace numeric IDs with full objects
                graphData.semi_weak_links.push({
                    ...link,
                    source: sourceNode,
                    target: targetNode,
                });
            } else {
                console.warn(
                    `Semi weak link source or target node not found in graphData.nodes. Source: ${link.source}, Target: ${link.target}`
                );
            }
        }
    });

    weakNewLinks.forEach((link) => {
        if (!weakLinkExists(link)) {
            const sourceNode = graphData.nodes.find((node) => node.id === link.source);
            const targetNode = graphData.nodes.find((node) => node.id === link.target);
            if (sourceNode && targetNode) {
                // Replace numeric IDs with full objects
                graphData.weak_links.push({
                    ...link,
                    source: sourceNode,
                    target: targetNode,
                });
            } else {
                console.warn(
                    `Weak link source or target node not found in graphData.nodes. Source: ${link.source}, Target: ${link.target}`
                );
            }
        }
    });


    console.log("Merge complete. Updated graph data:", graphData);
}

function parseLinkPubCount(link, journalRank, conferenceRank, fromYear, toYear){
    let yearSumAll = 0;
    let yearSumInRange = 0;
    let rankSum = 0;
    // Get the current year to enforce the upper limit
    const currentYear = new Date().getFullYear();

    const hasConferenceFilter = conferenceRank && conferenceRank.trim() !== "";
    const hasJournalFilter = journalRank && journalRank.trim() !== "";
    const userHasRankFilter = hasConferenceFilter || hasJournalFilter;

    const fromYearNum = fromYear ? parseInt(fromYear, 10) : NaN;
    const toYearNum = toYear ? parseInt(toYear, 10) : NaN;
    const userHasYearFilter = (!isNaN(fromYearNum) || !isNaN(toYearNum));

    // Helper to check if a property is within the valid range
    function isWithinYearRange(propKey) {
        const year = parseInt(propKey, 10);
        if (isNaN(year)) return false; // Not a numeric property
        if (year < 1950 || year > currentYear) return false; // Exclude out-of-range years
        if (!isNaN(fromYearNum) && year < fromYearNum) return false;
        return !(!isNaN(toYearNum) && year > toYearNum);
    }

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
}

// ======================================================
// Calculates pub_count for each link based on the rules of parseLinkPubCount:
// ======================================================
function updatePubCount(conferenceRank, journalRank, fromYear, toYear) {

    graphData.links.forEach((link) => parseLinkPubCount(link, journalRank, conferenceRank, fromYear, toYear));
    graphData.semi_weak_links.forEach((link) => parseLinkPubCount(link, journalRank, conferenceRank, fromYear, toYear));
    graphData.weak_links.forEach((link) => parseLinkPubCount(link, journalRank, conferenceRank, fromYear, toYear));

    console.log("New pub_count updated for links:", graphData.links);
    console.log("New pub_count updated for semi_weak_links:", graphData.semi_weak_links);
    console.log("New pub_count updated for weak_links:", graphData.weak_links);

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
    let semiWeakLinkData = graphData.semi_weak_links.filter((l) => l.pub_count && l.pub_count > 0);
    let weakLinkData = graphData.weak_links.filter((l) => l.pub_count && l.pub_count > 0);
    let nodeData = graphData.nodes.filter((n) => true);

    const linksCheckbox = document.getElementById('links-checkbox');

    if (linksCheckbox.checked) {
        if (conferenceRank && conferenceRank.trim() !== "") {
            linkData = linkData.filter((l) => typeof l[conferenceRank] === "number" && l[conferenceRank] > 0);
            semiWeakLinkData = semiWeakLinkData.filter((l) => typeof l[conferenceRank] === "number" && l[conferenceRank] > 0);
            weakLinkData = weakLinkData.filter((l) => typeof l[conferenceRank] === "number" && l[conferenceRank] > 0);
        }
        if (journalRank && journalRank.trim() !== "") {
            linkData = linkData.filter((l) => typeof l[journalRank] === "number" && l[journalRank] > 0);
            semiWeakLinkData = semiWeakLinkData.filter((l) => typeof l[journalRank] === "number" && l[journalRank] > 0);
            weakLinkData = weakLinkData.filter((l) => typeof l[journalRank] === "number" && l[journalRank] > 0);
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
    semiWeakLinkData = semiWeakLinkData.filter((link) => {
        const sourceId = typeof link.source === "object" ? link.source.id : link.source;
        const targetId = typeof link.target === "object" ? link.target.id : link.target;
        return nodeIds.has(sourceId) && nodeIds.has(targetId);
    });
    weakLinkData = weakLinkData.filter((link) => {
        const sourceId = typeof link.source === "object" ? link.source.id : link.source;
        const targetId = typeof link.target === "object" ? link.target.id : link.target;
        return nodeIds.has(sourceId) && nodeIds.has(targetId);
    });

    // Filter out nodes that are not linked
    const linkedNodeIds = new Set();
    const semiWeakLinkedNodeIds = new Set();
    const weakLinkedNodeIds = new Set();
    linkData.forEach((link) => {
        const sourceId = typeof link.source === "object" ? link.source.id : link.source;
        const targetId = typeof link.target === "object" ? link.target.id : link.target;
        linkedNodeIds.add(sourceId);
        linkedNodeIds.add(targetId);
    });
    semiWeakLinkData.forEach((link) => {
        const sourceId = typeof link.source === "object" ? link.source.id : link.source;
        const targetId = typeof link.target === "object" ? link.target.id : link.target;
        semiWeakLinkedNodeIds.add(sourceId);
        semiWeakLinkedNodeIds.add(targetId);
    });
    weakLinkData.forEach((link) => {
        const sourceId = typeof link.source === "object" ? link.source.id : link.source;
        const targetId = typeof link.target === "object" ? link.target.id : link.target;
        weakLinkedNodeIds.add(sourceId);
        weakLinkedNodeIds.add(targetId);
    });
    let filteredNodes = nodeData.filter((node) => linkedNodeIds.has(node.id) || weakLinkedNodeIds.has(node.id) || semiWeakLinkedNodeIds.has(node.id));

    if (!showAll){
        weakLinkData = weakLinkData.filter((link) => {
            const sourceId = typeof link.source === "object" ? link.source.id : link.source;
            const targetId = typeof link.target === "object" ? link.target.id : link.target;
            return currentlySelected.includes(targetId) && currentlySelected.includes(sourceId);
        });
        filteredNodes = filteredNodes.filter((node) => linkedNodeIds.has(node.id) || semiWeakLinkedNodeIds.has(node.id));
    }

    console.log("Nodes: " + filteredNodes.length + " - links - " + linkData.length + " - " + semiWeakLinkData.length + " - " + weakLinkData.length);
    // ----------------------------------------------------------------------
    // If we have no nodes or no links at this point, alert & stop.
    // ----------------------------------------------------------------------
    if ((filteredNodes.length === 0 || (linkData.length === 0 && weakLinkData.length === 0 && semiWeakLinkData.length === 0))
        && (graphData.nodes.length !== 0 && (graphData.links.length !== 0 || graphData.weak_links.length !== 0 || graphData.semi_weak_links.length !== 0))) {
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
        .force("semi_weak_link", d3.forceLink(semiWeakLinkData).id(d => d.id).distance(200).strength(d => (1 / d.root_counts)))
      .force("charge", d3.forceManyBody().strength(-1000))
      .force("center", d3.forceCenter(width / 2, height / 2))
      .force("collide", d3.forceCollide().radius(FORCE_SETTINGS.collideRadius).strength(FORCE_SETTINGS.collideStrength))
      .force("x", d3.forceX())
      .force("y", d3.forceY());

    simulation.on("end", () => {
         simulation
           .force("link", null)
             .force("semi_weak_link", null)
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
    const semiWeakLink = zoomLayer.append("g")
      .selectAll("line")
      .data(semiWeakLinkData)
      .join("line")
      .attr("stroke", (d) => getLinkColor(d, conferenceRank, journalRank, linksCheckbox.checked))
      .attr("stroke-width", (d) => LINK_WIDTH_SCALE(d.pub_count));
    const weakLink = zoomLayer.append("g")
      .selectAll("line")
      .data(weakLinkData)
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
        .attr("r", d => d.is_root ? 32 : 16);

    // A circle (visible outline)
    node.append("circle")
        .attr("r", d => d.is_root ? 32 : 16)
        .attr("fill", "white")
        .attr("stroke", "#000")
        .attr("stroke-width", 1.5);

    // The image, clipped to the circle
    node.append("svg:image")
        .attr("xlink:href", d => d.image || "")
        .attr("width", d => d.is_root ? 64 : 32)
        .attr("height", d => d.is_root ? 64 : 32)
        .attr("x", d => d.is_root ? -32 : -16)
        .attr("y", d => d.is_root ? -32 : -16)
        .attr("clip-path", d => `url(#clip-circle-${d.id})`)
        .on("error", function () {
            d3.select(this).attr("xlink:href", "/static/resource/avatar.png");
        });

    // The label below the node
    node.append("text")
        .attr("x", 0)
        .attr("y", d => d.is_root ? 48 : 24)
        .attr("text-anchor", "middle")
        .attr("alignment-baseline", "hanging")
        .style("font-size", d => d.is_root ? "12px" : "8px")
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

        weakLink
          .attr("x1", d => d.source.x)
          .attr("y1", d => d.source.y)
          .attr("x2", d => d.target.x)
          .attr("y2", d => d.target.y);

        semiWeakLink
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

    loadingPopup.style.display = "block";

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
function fetchGraphData(selectedNodeId, depth, conferenceRank, journalRank, fromYear, toYear, loadingPopup, render = true, init = false){
    prevConfRank = conferenceRank;
    prevJournalRank = journalRank;
    currentlySelected = selectedNodeId;
    fetch("/generate-graph", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
            start_author_id: selectedNodeId,
            depth: depth,
            conference_rank: conferenceRank,
            journal_rank: journalRank,
            from_year: fromYear,
            to_year: toYear
        }),
    })
        .then((response) => response.json())
        .then(({ nodes, links, semi_weak_links, weak_links }) => {
            console.log("API response received:", { nodes, links, semi_weak_links, weak_links });
            mergeGraphData(nodes, links, semi_weak_links, weak_links);
            updatePubCount(conferenceRank, journalRank, fromYear, toYear);
            updateNodeDropdown()
            if (render === true) {
                setTimeout(async () => {
                    renderGraph(conferenceRank, journalRank);
                }, 1000);
            }

            prev_id = selectedNodeId;
            prev_depth = depth;
        })
        .catch((error) => console.error("Error during graph generation:", error))
        .finally(() => {
            if (loadingPopup != null){
                loadingPopup.style.display = "none";
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
function clearGraph(){
    graphData.links.length = 0;
    graphData.nodes.length = 0;
    graphData.semi_weak_links.length = 0;
    graphData.weak_links.length = 0;
    const svg = d3.select("svg");
    svg.selectAll("*").remove();
}

document.getElementById("graph-form").addEventListener("submit", function (event) {
    event.preventDefault();
    console.log("Form submission intercepted.");

    const formData = new FormData(event.target);
    const selectedNodeId = $('#node-label').val();
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

    let selected = selectedNodeId.join(",");

    if (prev_id === selected && prev_depth === depth) {
        console.log("Skipping API call as prev_id and prev_depth match selectedNodeId and depth.");
        updatePubCount(conferenceRank, journalRank, fromYear, toYear);
        setTimeout(async () => {
            renderGraph(conferenceRank, journalRank);
        }, 1000);
    } else {
        // clear the graph
        clearGraph();
        console.log("Making API call as prev_id and prev_depth are different.");

        // Show the loading popup and start the timer
        loadingPopup.style.display = "block";

        fetchGraphData(selected, depth, conferenceRank, journalRank, fromYear, toYear, loadingPopup);
    }
});

document.addEventListener("DOMContentLoaded", () => {
    const checkbox = document.getElementById("links-checkbox");
    const label = document.getElementById("filter-label");

    const link_checkbox = document.getElementById("graph-links-checkbox");
    const link_label = document.getElementById("graph-filter-label");

    checkbox.addEventListener("change", () => {
        if (checkbox.checked) {
            label.textContent = "Filter Ranks By: Links";
        } else {
            label.textContent = "Filter Ranks By: Nodes";
        }
    });
    link_checkbox.addEventListener("change", () => {
        if (link_checkbox.checked) {
            showAll = true;
            link_label.textContent = "Show Links: All";
        } else {
            showAll = false;
            link_label.textContent = "Show Links: From Roots";
        }
        renderGraph(prevConfRank, prevJournalRank);
    });
});

// ======================================================
// Initialize dropdown and basic graph on page load
// ======================================================
function initGraph(init_ids) {
    try {
        // Make a single request with all option values concatenated
        fetchGraphData(init_ids, 1, "", "", "", "", null, false);
        console.log("Graph initialization request sent for all options.");
    } catch (error) {
        console.error("Error during graph initialization:", error);
    } finally {
        // generate graph data for the last id, just for rendering, it won't get generated twice and query is small

        setTimeout(async () => {
            fetchGraphData(init_ids, 1, null, null, null, null, null, true, true);
        }, 500);
    }
}

window.onload = function () {
    console.log("Initializing dropdown on page load...");

    let init_ids = startIds.join(',');
    let init_labels = startLabels.join(',');
    init_ids = init_ids.replaceAll("(", "").replaceAll(")", "");

    // Function to convert string to title case
    const toTitleCase = (str) => {
        return str
            .toLowerCase()
            .replace(/\b\w/g, (char) => char.toUpperCase());
    };

    // Convert init_labels to title case
    let titleCasedLabels = init_labels
        .split(',')
        .map(label => toTitleCase(label));

    let idArray = init_ids.split(',');
    let labelArray = titleCasedLabels;

    $(document).ready(function () {
        $('#node-label').select2({
            placeholder: 'Select a label...',
            allowClear: true
        });

        // Populate the select element
        const selectElement = $('#node-label');
        idArray.forEach((id, index) => {
            const label = "  "+labelArray[index] || ''; // Handle cases where lengths might differ
            selectElement.append(new Option(label, id, true, true)); // Select option by default
        });

        selectElement.trigger('change'); // Trigger the change event for select2
    });

    setTimeout(async () => {
        const loadingPopup = document.getElementById("loading-popup");
        if (!loadingPopup) {
            console.error("loadingPopup not found!");
        }

        // Show the loading popup and start the timer
        loadingPopup.style.display = "block";

        initGraph(init_ids);
        console.log("Graph initialization script has finished.");
    }, 500);
};







