document.getElementById("graph-form").addEventListener("submit", function (event) {
    event.preventDefault();

    const formData = new FormData(event.target);
    const maxDepth = formData.get("max_depth");
    const startAuthorId = document.querySelector(".graph-component").dataset.startId;

    fetch("/generate-graph", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ max_depth: maxDepth, start_author_id: startAuthorId })
    })
        .then(response => response.json())
        .then(data => {
            const nodes = data.nodes;
            const links = data.links;

            const svg = d3.select("svg");
            svg.selectAll("*").remove(); // Clear existing elements

            const width = 1000;
            const height = 600;

            const zoomLayer = svg.append("g"); // Create a layer for zooming and panning

            const simulation = d3.forceSimulation(nodes)
                .force("link", d3.forceLink(links).id(d => d.id))
                .force("charge", d3.forceManyBody().strength(-300)
                    .distanceMin(50) // Minimum distance for interaction
                    .distanceMax(500)) // Maximum distance where forces are effective)
                .force("center", d3.forceCenter(width / 1.5, height / 1.5));

            // Add zoom and drag behavior
            svg.call(d3.zoom()
                .scaleExtent([0.5, 2]) // Zoom levels
                .on("zoom", (event) => {
                    zoomLayer.attr("transform", event.transform);
                }));

            // Add links
            const link = zoomLayer.append("g")
                .selectAll("line")
                .data(links)
                .enter().append("line")
                .attr("stroke", "#999")
                .attr("stroke-width", 2);

            // Add nodes (images)
            const node = zoomLayer.append("g")
                .selectAll("g")
                .data(nodes)
                .enter().append("g") // Grouping each node's image and label
                .call(d3.drag()
                    .on("start", dragStarted)
                    .on("drag", dragged)
                    .on("end", dragEnded));

            // Append images to nodes
            node.append("image")
                .attr("xlink:href", d => d.image) // Load image from URL
                .attr("width", 90) // Adjust size of the image
                .attr("height", 90)
                .attr("x", -45) // Center the image horizontally
                .attr("y", -45); // Center the image vertically

            // Append labels to nodes
            node.append("text")
                .attr("font-size", "16px")
                .attr("fill", "#000")
                .attr("text-anchor", "middle")
                .attr("dy", 60) // Position the label below the image
                .text(d => d.label);

            simulation.on("tick", () => {
                // Update link positions
                link.attr("x1", d => d.source.x)
                    .attr("y1", d => d.source.y)
                    .attr("x2", d => d.target.x)
                    .attr("y2", d => d.target.y);

                // Update node group positions (affects both images and labels)
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

