# Eagle Documentation

Eagle documentation repository for content available on http://eagle.incubator.apache.org/docs/

Documentation is written in [Markdown](https://guides.github.com/features/mastering-markdown/) format and statically generated into HTML using [MkDocs](http://www.mkdocs.org/).  All documentation is located in the [docs](docs) directory, and [mkdocs.yml](mkdocs.yml) file describes the navigation structure of the published documentation.

## Installation & Preview
1. Install `MkDocs`: [http://www.mkdocs.org/#installation](http://www.mkdocs.org/#installation)
  > MkDocs is built upon Python, before starting to install MkDocs, please make sure you verify at least one python package management tool like `easy_install` or `pip` has been installed
  >- How to install easy_install? https://pypi.python.org/pypi/setuptools#installation-instructions
  >- How to install pipe? https://pip.readthedocs.org/en/stable/installing/#install-pip
  
     sudo easy_install install mkdocs
  
  or 
  
      sudo pip install mkdocs
  
2. Validate and preview existing documents

  cd incubator-eagle
  mkdocs serve -s

3. Open [http://127.0.0.1:8000](http://127.0.0.1:8000) 

## Authoring

New pages can be added under [eagle-docs](.) or related sub-category, and a reference to the new page must be added to the [mkdocs.yml](../mkdocs.yml#L36) file to make it availabe in the navigation.  Embedded images are typically added to images folder at the same level as the new page.

When creating or editing pages, it can be useful to see the live results, and how the documents will appear when published.  Live preview feature is available by running the following command at the root of the repository:

```bash
mkdocs serve
```

For additional details see [writing your docs](http://www.mkdocs.org/user-guide/writing-your-docs/) guide.

## Site Configuration

Guides on applying site-wide [configuration](http://www.mkdocs.org/user-guide/configuration/) and [themeing](http://www.mkdocs.org/user-guide/styling-your-docs/) are available on the MkDocs site.

## Deployment

Deployment is done in two steps.  First all documentation is statically generatd into HTML files and then it is deployed to the eagle website.  For more details on how conversion to HTML works see [MkDocs documentation](http://www.mkdocs.org/).

1.  Go to release branch of the repository and execute the following command to build the docs.  **Note**: Until [mkdocs #859](https://github.com/mkdocs/mkdocs/issues/859) is resolved and available for download, use mkdocs built against [master](https://github.com/mkdocs/mkdocs).

```bash
# set project version
EAGLE_VERSION=0.3.0

# build docs under site foolder
mkdocs build --clean

# copy docs from site into target folder on eagle-site
cd ../incubator-eagle-svn
cp -r ../incubator-eagle/site site/docs/${EAGLE_VERSION}
# Set this to be latest available docs version
git add -A
git commit -m "Adding eagle-${EAGLE_VERSION} documentation"
git push
```

2. Add link to landing site
> `TODO`
