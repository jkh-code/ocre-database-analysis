# Project Notes

## Open questions
- Does a user agent need to be added to prevent any kind of exceptions? If a user agent is required, this link may be helpful for selecting a user agent: https://techblog.willshouse.com/2012/01/03/most-common-user-agents/



## Next steps
- Replace print statements with logging.
- Current design will take over 72 hours (3 days) to scape all 228k images using `download_images` method. Implement multiprocessing to reduce this time. Until multiprocessing is implemented, images will not be downloaded in final script.
- Redesign `scrape_uris_pagination` method to scrape pages in less time.
