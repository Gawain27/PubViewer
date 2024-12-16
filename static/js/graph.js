let graphData = {
    nodes: [],
    links: []
};

// Function to populate the dropdown with node labels
function populateDropdown() {
    const dropdown = document.getElementById("node-label");

    // Remove existing non-default options
    const defaultOption = dropdown.querySelector("option[selected]");
    dropdown.innerHTML = defaultOption.outerHTML;

    graphData.nodes.forEach(node => {
        if (!dropdown.querySelector(`option[value="${node.id}"]`)) {
            const option = document.createElement("option");
            option.value = node.id;
            option.textContent = node.label;
            dropdown.appendChild(option);
        }
    });
}

document.getElementById("graph-form").addEventListener("submit", function (event) {
    event.preventDefault();

    const formData = new FormData(event.target);
    const selectedNodeId = formData.get("node_label");

    fetch("/generate-graph", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ start_author_id: selectedNodeId })
    })
        .then(response => response.json())
        .then(data => {
            const newNodes = data.nodes;
            const newLinks = data.links;

            // Merge new nodes
            newNodes.forEach(newNode => {
                if (!graphData.nodes.some(node => node.id === newNode.id)) {
                    graphData.nodes.push(newNode);
                }
            });

            // Merge new links
            newLinks.forEach(newLink => {
                if (!graphData.links.some(link =>
                    (link.source === newLink.source && link.target === newLink.target) ||
                    (link.source === newLink.target && link.target === newLink.source)
                )) {
                    graphData.links.push(newLink);
                }
            });

            // Populate dropdown with updated nodes
            populateDropdown();

            // Render graph
            const svg = d3.select("svg");
            svg.selectAll("*").remove();

            const width = 1000;
            const height = 600;

            const zoomLayer = svg.append("g");

            const simulation = d3.forceSimulation(graphData.nodes)
                .force("link", d3.forceLink(graphData.links).id(d => d.id).distance(800))
                .force("charge", d3.forceManyBody().strength(-800))
                .force("center", d3.forceCenter(width / 1.5, height / 1.5))
                .force("collision", d3.forceCollide(120)); // Avoid overlap

            svg.call(d3.zoom()
                .scaleExtent([0.2, 3])
                .on("zoom", (event) => {
                    zoomLayer.attr("transform", event.transform);
                }));

            const link = zoomLayer.append("g")
                .selectAll("line")
                .data(graphData.links)
                .enter().append("line")
                .attr("stroke", "#999")
                .attr("stroke-width", 2);

            const node = zoomLayer.append("g")
                .selectAll("g")
                .data(graphData.nodes)
                .enter().append("g")
                .call(d3.drag()
                    .on("start", dragStarted)
                    .on("drag", dragged)
                    .on("end", dragEnded));

            // Add a clipPath for each node
            node.append("clipPath")
                .attr("id", d => `clip-${d.id}`)
                .append("circle")
                .attr("r", 50); // Circle radius for clipping

            // Add a circle for the background
            node.append("circle")
                .attr("r", 50) // Circle radius
                .attr("fill", "white") // Circle fill color
                .attr("stroke", "#999") // Circle border color
                .attr("stroke-width", 2);

            // Add images clipped by the circle
            node.append("image")
                .attr("xlink:href", d => d.image)
                .attr("width", 100) // Ensure the image covers the circle
                .attr("height", 100)
                .attr("x", -50) // Center the image
                .attr("y", -50) // Center the image
                .attr("clip-path", d => `url(#clip-${d.id})`); // Apply the clipping path

            node.append("text")
                .attr("font-size", "16px")
                .attr("fill", "#000")
                .attr("text-anchor", "middle")
                .attr("dy", 70) // Position text below the image
                .text(d => d.label);

            simulation.on("tick", () => {
                link.attr("x1", d => d.source.x)
                    .attr("y1", d => d.source.y)
                    .attr("x2", d => d.target.x)
                    .attr("y2", d => d.target.y);

                node.attr("transform", d => `translate(${d.x},${d.y})`);
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
        })
        .catch(err => console.error(err));
});

// Populate the dropdown initially
populateDropdown();


