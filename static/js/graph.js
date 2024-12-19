const GRAPH_DIMENSIONS = { width: 1000, height: 600 };
const FORCE_SETTINGS = {
    linkDistance: 200,
    linkStrength: 1,
    chargeStrength: -300,
    collideRadius: 60,
    collideStrength: 1,
};
let graphData = { nodes: [], links: [] };

// Updates the dropdown labels with graph nodes
function updateNodeDropdown() {
    const nodeLabelDropdown = document.getElementById("node-label");
    const defaultOption = nodeLabelDropdown.querySelector("option[selected]");
    nodeLabelDropdown.innerHTML = defaultOption.outerHTML;

    graphData.nodes.forEach(({ id, label }) => {
        if (!nodeLabelDropdown.querySelector(`option[value="${id}"]`)) {
            const newOption = document.createElement("option");
            newOption.value = id;
            newOption.textContent = label;
            nodeLabelDropdown.appendChild(newOption);
        }
    });
}

// Merges new nodes into existing graph data while avoiding duplication
function mergeGraphData(newNodes, newLinks) {
    const alreadyExists = (arr, key) => arr.some((item) => item.id === key);
    const linkExists = (link) =>
        graphData.links.some(
            (gLink) =>
                (gLink.source === link.source && gLink.target === link.target) ||
                (gLink.source === link.target && gLink.target === link.source)
        );

    newNodes.forEach((node) => {
        if (!alreadyExists(graphData.nodes, node.id)) graphData.nodes.push(node);
    });
    newLinks.forEach((link) => {
        if (!linkExists(link)) graphData.links.push(link);
    });
}

// Renders the graph using D3.js
function renderGraph() {
    const svg = d3.select("svg");
    svg.selectAll("*").remove();

    const zoomLayer = svg.append("g");
    const simulation = d3
        .forceSimulation(graphData.nodes)
        .force("link", d3.forceLink(graphData.links).id((d) => d.id).distance(FORCE_SETTINGS.linkDistance).strength(FORCE_SETTINGS.linkStrength))
        .force("charge", d3.forceManyBody().strength(FORCE_SETTINGS.chargeStrength))
        .force("center", d3.forceCenter(GRAPH_DIMENSIONS.width / 2, GRAPH_DIMENSIONS.height / 2))
        .force("collide", d3.forceCollide().radius(FORCE_SETTINGS.collideRadius).strength(FORCE_SETTINGS.collideStrength))
        .alphaDecay(0.05);

    svg.call(
        d3.zoom().scaleExtent([0.2, 3]).on("zoom", (event) => zoomLayer.attr("transform", event.transform))
    );

    const link = zoomLayer
        .append("g")
        .selectAll("line")
        .data(graphData.links)
        .enter()
        .append("line")
        .attr("stroke", "#999")
        .attr("stroke-width", 2);

    const node = zoomLayer
        .append("g")
        .selectAll("g")
        .data(graphData.nodes)
        .enter()
        .append("g")
        .call(
            d3
                .drag()
                .on("start", dragStarted)
                .on("drag", dragged)
                .on("end", dragEnded)
        );

    node.append("clipPath")
        .attr("id", (d) => `clip-${d.id}`)
        .append("circle")
        .attr("r", 50);
    node.append("circle").attr("r", 50).attr("fill", "white").attr("stroke", "#999").attr("stroke-width", 2);
    node.append("image")
        .attr("xlink:href", (d) => d.image)
        .attr("width", 100)
        .attr("height", 100)
        .attr("x", -50)
        .attr("y", -50)
        .attr("clip-path", (d) => `url(#clip-${d.id})`);
    node.append("text").attr("font-size", "16px").attr("fill", "#000").attr("text-anchor", "middle").attr("dy", 70).text((d) => d.label);

    simulation.on("tick", () => {
        link.attr("x1", (d) => d.source.x).attr("y1", (d) => d.source.y).attr("x2", (d) => d.target.x).attr("y2", (d) => d.target.y);
        node.attr("transform", (d) => `translate(${d.x},${d.y})`);
    });

    function dragStarted(event, d) {
        if (!event.active) simulation.alphaTarget(0.3).restart();
        d.fx = d.x;
        d.fy = d.y;
    }

    function dragged(event, d) {
        d.fx = event.x;
        d.fy = event.y;
    }

    function dragEnded(event, d) {
        if (!event.active) simulation.alphaTarget(0);
        d.fx = null;
        d.fy = null;
    }
}

// Handles form submission for graph generation
document.getElementById("graph-form").addEventListener("submit", function (event) {
    event.preventDefault();
    const selectedNodeId = new FormData(event.target).get("node_label");

    fetch("/generate-graph", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ start_author_id: selectedNodeId }),
    })
        .then((response) => response.json())
        .then(({ nodes, links }) => {
            mergeGraphData(nodes, links);
            updateNodeDropdown();
            renderGraph();
        })
        .catch(console.error);
});

// Initialize dropdown on page load
updateNodeDropdown();



