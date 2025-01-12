from quart import render_template

from com.gwngames.server.query.QueryBuilder import QueryBuilder

class GeneralDetailOverview:
    def __init__(self, query_builder: QueryBuilder, title_field, description_field, image_field=None, url_fields=None):
        """
        Initialize the DetailedDataViewer.

        :param query_builder: An instance of QueryBuilder configured for the query.
        :param title_field: Field name for the title.
        :param description_field: Field name for the description.
        :param image_field: (Optional) Field name for the image URL.
        :param url_fields: (Optional) List of field names that contain URLs.
        """
        self.query_builder = query_builder
        self.title_field = title_field
        self.description_field = description_field
        self.image_field = image_field
        self.url_fields = url_fields or []
        self.row_methods = []

    def add_row_method(self, label, endpoint_name, column_name="id"):
        """
        Add a method to be triggered by a link in the detail view.
        """
        self.row_methods.append({
            "label": label,
            "endpoint": endpoint_name,
            "column_name": column_name
        })

    async def render(self):
        """
        Render the data viewer component with the configured query.
        """
        # Execute the query to fetch data
        rows = await self.query_builder.execute()
        if not rows:
            return render_template("generic_detail_overview.html", data=None)

        # Take the first result as the primary data source
        data = rows[0]

        # Extract image, title, and description
        image_url = data.get(self.image_field) if self.image_field else None
        title = data.get(self.title_field)
        description = data.get(self.description_field)

        # Prepare remaining fields for the table
        details = {
            k: v for k, v in data.items()
            if k not in [self.title_field, self.description_field, self.image_field]
        }

        return await render_template(
            "generic_detail_overview.html",
            image_url=image_url,
            title=title,
            description=description,
            is_description_url=self.description_field in self.url_fields,
            details=details,
            data=data,
            row_methods=self.row_methods,
            # Pass the url_fields so the template knows which columns are URLs
            url_fields=self.url_fields
        )

