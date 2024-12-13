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

            const simulation = d3.forceSimulation(nodes)
                .force("link", d3.forceLink(links).id(d => d.id))
                .force("charge", d3.forceManyBody().strength(-500))
                .force("center", d3.forceCenter(400, 300));

            // Add links with labels
            const link = svg.append("g")
                .selectAll("line")
                .data(links)
                .enter().append("line")
                .attr("stroke", "#999")
                .attr("stroke-width", 2);

            const linkLabels = svg.append("g")
                .selectAll("text")
                .data(links)
                .enter().append("text")
                .attr("font-size", "12px")
                .attr("fill", "#000")
                .text(d => d.label);

            // Add nodes
            const node = svg.append("g")
                .selectAll("circle")
                .data(nodes)
                .enter().append("circle")
                .attr("r", 10) // Increased node size
                .attr("fill", "#69b3a2")
                .call(d3.drag()
                    .on("start", dragStarted)
                    .on("drag", dragged)
                    .on("end", dragEnded));

            // Add labels to nodes
            const nodeLabels = svg.append("g")
                .selectAll("text")
                .data(nodes)
                .enter().append("text")
                .attr("font-size", "14px")
                .attr("fill", "#000")
                .attr("dy", "-1.5em") // Position above the nodes
                .text(d => d.label);

            simulation.on("tick", () => {
                // Update link positions
                link.attr("x1", d => d.source.x)
                    .attr("y1", d => d.source.y)
                    .attr("x2", d => d.target.x)
                    .attr("y2", d => d.target.y);

                // Update link label positions
                linkLabels.attr("x", d => (d.source.x + d.target.x) / 2)
                    .attr("y", d => (d.source.y + d.target.y) / 2);

                // Update node positions
                node.attr("cx", d => d.x)
                    .attr("cy", d => d.y);

                // Update node label positions
                nodeLabels.attr("x", d => d.x)
                    .attr("y", d => d.y);
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

