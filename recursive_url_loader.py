from typing import Iterator, List, Optional, Set
from urllib.parse import urlparse

import requests
import logging

from langchain.docstore.document import Document
from langchain.document_loaders.base import BaseLoader


class RecursiveUrlLoader(BaseLoader):
    """Loader that loads all child links from a given url."""

    def __init__(self, url: str, exclude_dirs: Optional[str] = None) -> None:
        """Initialize with URL to crawl and any sub-directories to exclude."""
        self.url = url
        self.exclude_dirs = exclude_dirs

    def get_child_links_recursive(
        self, url: str
    )->Iterator[Document]:
        """Recursively get all child links starting with the path of the input URL."""

        try:
            from bs4 import BeautifulSoup
        except ImportError:
            raise ImportError(
                "The BeautifulSoup package is required for the RecursiveUrlLoader."
            )

        base_path = urlparse(url).path
        to_visit = [url]
        visited = set()
        visited_cnt = 0
        found_cnt = 0
        visited.add(url)
        visited_cnt += 1
        while(len(to_visit) > 0):
            url = to_visit.pop()
            logging.info(f"get_child_links_recursive: visit self.url:{self.url}, url:{url}, visited_cnt:{visited_cnt},found_cnt:{found_cnt}")
            # Construct the base and parent URLs
            parsed_url = urlparse(url)
            base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
            parent_url = "/".join(parsed_url.path.split("/")[:-1])
            current_path = parsed_url.path

            # Add a trailing slash if not present
            if not base_url.endswith("/"):
                base_url += "/"
            if not parent_url.endswith("/"):
                parent_url += "/"

            # Exclude the links that start with any of the excluded directories
            if self.exclude_dirs and any(
                url.startswith(exclude_dir) for exclude_dir in self.exclude_dirs
            ):
                continue

            # Get all links that are relative to the root of the website
            try:
                response = requests.get(url)
                if response.text:
                    found_cnt += 1
                    yield Document(page_content=response.text, metadata={'source':url})
                soup = BeautifulSoup(response.text, "html.parser")
                all_links = [link.get("href") for link in soup.find_all("a")]

                # Extract only the links that are children of the current URL
                child_links = list(
                    {
                        link
                        for link in all_links
                        if link and link.startswith(base_path) and link != base_path
                    }
                )

                # Get absolute path for all root relative links listed
                absolute_paths = [
                    f"{urlparse(base_url).scheme}://{urlparse(base_url).netloc}{link}"
                    for link in child_links
                ]

                # Store the visited links and recursively visit the children
                for link in absolute_paths:
                    # Check all unvisited links
                    if link not in visited:
                        visited.add(link)
                        visited_cnt += 1
                        to_visit.append(link)
            except Exception as e:
                logging.exception(e)

    def lazy_load(self) -> Iterator[Document]:
        return self.get_child_links_recursive(self.url)

    def load(self) -> List[Document]:
        """Load web pages."""
        return list(self.lazy_load())
