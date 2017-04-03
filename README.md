# Eagle Documentation Site
Welcome to the Apache Eagle documentation!

## Prerequisites

Install [jekyll](https://jekyllrb.com/) gem

    $ gem install jekyll

Generate the site, and start a server locally:

    $ jekyll serve -w
  
The `-w` option tells jekyll to watch for changes to files and regenerate the site automatically when any content changes.

Point your browser to [http://localhost:4000](http://localhost:4000)

By default, jekyll will generate the site in a `_site` directory.

## Editing documentations
1. Create a markdown file and add following content in header

        ---
        layout: doc
        title:  "Eagle Overview" 
        permalink: /docs/some-new-doc.html
        ---
        
        More content here ..
    
2. Register the link in documentations sidebar navigation

        documentations:
          - category: "Some category"
            links:
              - title: Some new doc
                url: /docs/some-new-doc.html

## Publishing the Apache Website
In order to publish the website, you must have committer access to Eagle's subversion repository.

The Eagle website is published using Apache svnpubsub. Any changes committed to subversion will be automatically published to eagle.apache.org.

To publish changes, tell jekyll to generate the site in the publish directory of subversion, then commit the changes:

    cd docs
    jekyll build -d /path/to/svn/repo/publish
    cd /path/to/svn/repo/publish
    svn commit
