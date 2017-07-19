# NCJ template api


## How to write a template

#### 1. Add application if not already there

If the application that need to be run is not already there do the following:
- Add it to the index.json with id, name, description
- Create a folder with the id of the application
- Add an icon in the root of the newly created folder(svg). Check for the license.
- Create a index.json inside the folder too

### 2. Add the action and templates

Application can have multiple actions(e.g. Render a movie, render a frame, etc.)
- Add the action to the index.json of the application
- Create a folder with the action id
- Inside create a `job.template.json` and a `pool.template.json` file and include the pool and job template. (We won't actually use the pool template directly it will be merged with the job template to make a autopool template)


Note on the pool tempalte:

All pool templates must have the following parameters:
- `numberNodes`: With a default value
- `vmSize`: With a default value