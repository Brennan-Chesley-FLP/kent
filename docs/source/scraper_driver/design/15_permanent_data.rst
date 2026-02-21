Step 15: Permanent Request Data
=================================

In previous steps, each request was independent - cookies and headers had to be
manually set on every request. This made authentication workflows cumbersome.

In Step 18, we introduce **permanent request data** that persists across the
entire request chain, making authentication and session management seamless.


Overview
--------

In this step, we introduce:

1. **permanent dict** - Added to Request
2. **Automatic inheritance** - Child requests inherit parent's permanent data
3. **Merge semantics** - Child can override or extend parent's permanent data
4. **Driver integration** - Permanent data applied to HTTP requests



Usage Examples
--------------

Bearer Token Authentication
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    class APIScrap(BaseScraper):
        def get_entry(self) -> Request:
            return Request(
                url="https://api.example.com/login",
                continuation="parse_login",
            )

        def parse_login(self, response: Response):
            # Extract token from login response
            data = json.loads(response.text)
            token = data["access_token"]

            # Set permanent header - flows to all descendants
            yield Request(
                url="/api/users",
                permanent={"headers": {"Authorization": f"Bearer {token}"}},
                continuation="parse_users",
            )

        def parse_users(self, response: Response):
            # Token automatically included in request!
            users = json.loads(response.text)
            for user in users:
                yield Request(
                    url=f"/api/users/{user['id']}",
                    continuation="parse_user_detail",
                )

        def parse_user_detail(self, response: Response):
            # Still has token!
            yield ParsedData(data=json.loads(response.text))


Session Cookie Authentication
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    class SessionScraper(BaseScraper):
        def get_entry(self) -> Request:
            return Request(
                url="https://example.com/login",
                continuation="parse_login",
            )

        def parse_login(self, response: Response):
            # Extract session cookie from login
            session_id = extract_cookie(response, "sessionid")

            # Set permanent cookie
            yield Request(
                url="/dashboard",
                permanent={"cookies": {"sessionid": session_id}},
                continuation="parse_dashboard",
            )

        def parse_dashboard(self, response: Response):
            # Cookie automatically included!
            for link in extract_links(response):
                yield Request(
                    url=link,
                    continuation="parse_page",
                )

        def parse_page(self, response: Response):
            # Still authenticated!
            yield ParsedData(data=extract_data(response))



Next Steps
----------

In :doc:`16_step_decorators`, we introduce the @step decorator for scraper
methods. This decorator uses argument inspection to automatically inject
parsed content (lxml trees, JSON, text) and request context, reducing
boilerplate and enabling callable continuations.
