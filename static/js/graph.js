const GRAPH_DIMENSIONS = { width: 1000, height: 600 };
const FORCE_SETTINGS = {
    linkDistance: 200,
    linkStrength: 1,
    chargeStrength: -300,
    collideRadius: 60,
    collideStrength: 1,
};
let graphData = { nodes: [], links: [] };
let prev_id = 0;
let prev_depth = 0;

// Updates the dropdown labels with graph nodes
function updateNodeDropdown() {
    console.log("Updating node dropdown...");
    const nodeLabelDropdown = document.getElementById("node-label");
    if (!nodeLabelDropdown) {
        console.error("Node label dropdown element not found!");
        return;
    }

    // Keep the default option (selected="selected") if it exists
    const defaultOption = nodeLabelDropdown.querySelector("option[selected]");
    console.log("Default option retained:", defaultOption?.outerHTML);

    nodeLabelDropdown.innerHTML = defaultOption.outerHTML;

    graphData.nodes.forEach(({ id, label }) => {
        if (!nodeLabelDropdown.querySelector(`option[value="${id}"]`)) {
            console.log(`Adding new option to dropdown: ID=${id}, Label=${label}`);
            const newOption = document.createElement("option");
            newOption.value = id;
            newOption.textContent = label;
            nodeLabelDropdown.appendChild(newOption);
        }
    });
    console.log("Dropdown update complete.");
}

// Merges new nodes into existing graph data while avoiding duplication
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
            console.log("Adding new node:", node);
            graphData.nodes.push(node);
        } else {
            console.log("Node already exists, skipping:", node);
        }
    });

    // Add links
    newLinks.forEach((link) => {
        if (!linkExists(link)) {
            console.log("Adding new link:", link);
            graphData.links.push(link);
        } else {
            console.log("Link already exists, skipping:", link);
        }
    });

    console.log("Merge complete. Updated graph data:", graphData);
}

/**
 * Calculates pub_count for each link based on the provided filters.
 *
 * @param {string} fromYear       - user input (could be "")
 * @param {string} toYear         - user input (could be "")
 * @param {string} conferenceRank - user input (could be "")
 * @param {string} journalRank    - user input (could be "")
 */
function updatePubCount(fromYear, toYear, conferenceRank, journalRank) {
    console.log("Updating pub_count for links...");
    console.log(`Input Parameters - fromYear: ${fromYear}, toYear: ${toYear}, conferenceRank: ${conferenceRank}, journalRank: ${journalRank}`);

    const fromYearNum = fromYear ? parseInt(fromYear, 10) : NaN;
    const toYearNum   = toYear   ? parseInt(toYear, 10)   : NaN;

    console.log(`Parsed Year Range - fromYearNum: ${fromYearNum}, toYearNum: ${toYearNum}`);

    // True if fromYear and toYear are both empty -> sum all numeric keys (years)
    const noYearRange = (!fromYear || fromYear.trim() === "") && (!toYear || toYear.trim() === "");
    console.log(`No Year Range: ${noYearRange}`);

    // True if conferenceRank and journalRank are both empty -> sum all numeric values that are not numeric keys
    const noRanks = (!conferenceRank || conferenceRank.trim() === "") && (!journalRank || journalRank.trim() === "");
    console.log(`No Ranks Filter: ${noRanks}`);


    function yearInRange(propKey) {
        // If we are ignoring year range, let everything in
        if (noYearRange) return true;

        const year = parseInt(propKey, 10);
        if (isNaN(year)) return false; // not a numeric property name
        if (!isNaN(fromYearNum) && year < fromYearNum) return false;
        return !(!isNaN(toYearNum) && year > toYearNum);
    }

    graphData.links.forEach((link, index) => {
        console.log(`Processing link ${index}:`, link);

        let sum = 0;

        // 1. If noRanks is true, sum all numeric values for property keys that are NOT numeric keys
        //    (excluding "source" and "target").
        //    Otherwise, sum only the selected rank's numeric values (conferenceRank/journalRank) if provided.
        if (noRanks) {
            Object.keys(link).forEach((propKey) => {
                // skip if property key is numeric (year), or source/target
                if (
                    propKey === "source" ||
                    propKey === "target" ||
                    !isNaN(parseInt(propKey)) // skip numeric property keys
                ) {
                    return;
                }
                // if the property value is numeric, add it
                if (typeof link[propKey] === "number") {
                    console.log(`Adding non-year numeric value from ${propKey}: ${link[propKey]}`);
                    sum += link[propKey];
                }
            });
        } else {
            // If conferenceRank is provided, add that property if numeric
            if (conferenceRank && link.hasOwnProperty(conferenceRank)) {
                const val = link[conferenceRank];
                if (typeof val === "number") {
                    console.log(`Adding conferenceRank value (${conferenceRank}): ${val}`);
                    sum += val;
                }
            }
            // If journalRank is provided, add that property if numeric
            if (journalRank && link.hasOwnProperty(journalRank)) {
                const val = link[journalRank];
                if (typeof val === "number") {
                    console.log(`Adding journalRank value (${journalRank}): ${val}`);
                    sum += val;
                }
            }
        }

        // 2. Handle numeric property keys (which we assume are "year" properties).
        //    If noYearRange = true, sum them all; otherwise, sum only those in [fromYear, toYear].
        Object.keys(link).forEach((propKey) => {
            // skip known non-year keys
            if (propKey === "source" || propKey === "target") return;

            // Check if the property key is numeric (e.g. "2020")
            if (!isNaN(parseInt(propKey, 10)) && yearInRange(propKey)) {
                const val = link[propKey];
                if (typeof val === "number") {
                    console.log(`Adding year value (${propKey}): ${val}`);
                    sum += val;
                }
            }
        });

        // Store the sum in the link
        console.log(`Final pub_count for link ${index}: ${sum}`);
        link.pub_count = sum;
    });

    console.log("pub_count updated for links:", graphData.links);
}

// Renders the graph using D3.js
function renderGraph() {
    console.log("Rendering graph...");
    const svg = d3.select("svg");
    svg.selectAll("*").remove();

    const zoomLayer = svg.append("g");
    console.log("Initialized zoom layer.");

    const simulation = d3
        .forceSimulation(graphData.nodes)
        .force("link", d3.forceLink(graphData.links).id((d) => d.id)
            .distance(FORCE_SETTINGS.linkDistance)
            .strength(FORCE_SETTINGS.linkStrength)
        )
        .force("charge", d3.forceManyBody().strength(FORCE_SETTINGS.chargeStrength))
        .force("center", d3.forceCenter(GRAPH_DIMENSIONS.width / 2, GRAPH_DIMENSIONS.height / 2))
        .force("collide", d3.forceCollide().radius(FORCE_SETTINGS.collideRadius).strength(FORCE_SETTINGS.collideStrength))
        .alphaDecay(0.05);

    // Enable zoom
    svg.call(
        d3.zoom().scaleExtent([0.2, 3]).on("zoom", (event) => {
            console.log("Zoom event:", event.transform);
            zoomLayer.attr("transform", event.transform);
        })
    );
    console.log("Configured D3 simulation and zoom.");

    // Only render links with pub_count > 0
    const linkData = graphData.links.filter((l) => l.pub_count && l.pub_count > 0);

    const link = zoomLayer
        .append("g")
        .selectAll("line")
        .data(linkData)
        .enter()
        .append("line")
        .attr("stroke", "#999")
        .attr("stroke-width", 2);
    console.log("Rendered links:", linkData);

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
        );

    console.log("Rendered nodes:", graphData.nodes);

    // For a circular "avatar" style node
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
        .attr("clip-path", (d) => `url(#clip-${d.id})`);

    node.append("text")
        .attr("font-size", "16px")
        .attr("fill", "#000")
        .attr("text-anchor", "middle")
        .attr("dy", 70)
        .text((d) => d.label);

    // Sync the node and link positions on each simulation tick
    simulation.on("tick", () => {
        link
            .attr("x1", (d) => d.source.x)
            .attr("y1", (d) => d.source.y)
            .attr("x2", (d) => d.target.x)
            .attr("y2", (d) => d.target.y);

        node.attr("transform", (d) => `translate(${d.x},${d.y})`);
    });
    console.log("Simulation tick handler attached.");

    function dragStarted(event, d) {
        console.log("Drag started for node:", d);
        if (!event.active) simulation.alphaTarget(0.3).restart();
        d.fx = d.x;
        d.fy = d.y;
    }

    function dragged(event, d) {
        console.log("Dragging node:", d);
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

// Handles form submission for graph generation
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

    // Check if prev_id and prev_depth are equal to selectedNodeId and depth
    if (prev_id === selectedNodeId && prev_depth === depth) {
        console.log("Skipping API call as prev_id and prev_depth match selectedNodeId and depth.");
        updatePubCount(fromYear, toYear, conferenceRank, journalRank);
        renderGraph();
    } else {
        console.log("Making API call as prev_id and prev_depth are different.");

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
                updatePubCount(fromYear, toYear, conferenceRank, journalRank);
                updateNodeDropdown();
                renderGraph();

                // Update prev_id and prev_depth
                prev_id = selectedNodeId;
                prev_depth = depth;
            })
            .catch((error) => console.error("Error during graph generation:", error));
    }
});


// Initialize dropdown on page load
console.log("Initializing dropdown on page load...");
updateNodeDropdown();




