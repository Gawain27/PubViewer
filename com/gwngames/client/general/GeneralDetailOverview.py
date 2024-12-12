from flask import render_template, request

class GeneralDetailOverview:
    def __init__(self, query_builder, title_field, description_field, image_field=None):
        """
        Initialize the DetailedDataViewer.

        :param query_builder: An instance of QueryBuilder configured for the query.
        :param title_field: Field name for the title.
        :param description_field: Field name for the description.
        :param image_field: (Optional) Field name for the image URL.
        """
        self.query_builder = query_builder
        self.title_field = title_field
        self.description_field = description_field
        self.image_field = image_field
        self.row_methods = []

    def add_row_method(self, label, endpoint_name, column_name="id"):
        """
        Add a method to be triggered by a link in the detail view.

        :param label: The label of the button/link.
        :param endpoint_name: The Flask endpoint to call when the link is clicked.
        """
        self.row_methods.append({"label": label, "endpoint": endpoint_name,
            "column_name": column_name})


    def render(self):
        """
        Render the data viewer component with the configured query.

        :return: Rendered HTML for the detailed data viewer.
        """
        # Execute the query to fetch data
        rows = self.query_builder.execute()
        if not rows:
            return render_template("generic_detail_overview.html", data=None)

        # Take the first result as the primary data source
        data = rows[0]

        # Extract image, title, and description
        image_url = data.get(self.image_field) if self.image_field else None
        title = data.get(self.title_field)
        description = data.get(self.description_field)

        # Prepare remaining fields for the table
        details = {k: v for k, v in data.items() if k not in [self.title_field, self.description_field, self.image_field]}

        return render_template(
            "generic_detail_overview.html",
            image_url=image_url,
            title=title,
            description=description,
            details=details,
            data=data,
            row_methods=self.row_methods
        )
