# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import sys

# -- Path setup --------------------------------------------------------------
sys.path.insert(0, os.path.abspath("../.."))

# -- Project information -----------------------------------------------------
project = "kent"
copyright = "2025, Free Law Project"
author = "Free Law Project"

# -- General configuration ---------------------------------------------------
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
    "sphinxcontrib.mermaid",
    "sphinx_immaterial",
]

templates_path = ["_templates"]
exclude_patterns: list[str] = []

# -- Options for HTML output -------------------------------------------------
html_theme = "sphinx_immaterial"
html_static_path: list[str] = []

# sphinx-immaterial theme options
html_theme_options = {
    "font": False,
    "repo_url": "https://github.com/freelawproject/kent",
    "repo_name": "freelawproject/kent",
    "features": [
        "search.suggest",
        "search.highlight",
        "search.share",
        "navigation.expand",
        "navigation.top",
        "toc.follow",
    ],
}

# -- Extension configuration -------------------------------------------------

# Napoleon settings (for Google/NumPy style docstrings)
napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = True
napoleon_use_param = True
napoleon_use_rtype = True

# Autodoc settings
autodoc_default_options = {
    "members": True,
    "member-order": "bysource",
    "special-members": "__init__",
    "undoc-members": True,
    "exclude-members": "__weakref__",
}
autodoc_typehints = "description"
autodoc_class_signature = "separated"

# Intersphinx mapping
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}

# RST prolog - substitutions available in all documents
rst_prolog = """
.. |check| unicode:: U+2714

.. |x| unicode:: U+2718
"""
