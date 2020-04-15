## Coding Exercise Solutions

#### Quick Links
1. Data profiling [notebook](./src/profile_data.ipynb). Also in [HTML](./notebooks) format
2. Automated process code source: [./src/main.py](./src/main.py)
3. [Mapbox](https://api.mapbox.com/styles/v1/aortega04/ck908h7be005k1iny7a6ztux3/draft.html?fresh=true&title=view&access_token=pk.eyJ1IjoiYW9ydGVnYTA0IiwiYSI6ImNrOHdlNW1heDBkaHczZm1jYmRmOWNobnUifQ.P1NVUDHPJ4DZb1g_XQRAzQ)
render of points and walking distance overlap areas of 5 min (red) and 10 min (blue)
4. Count of overlaps per point [./data/cleaned_data.json](./data/cleaned_data.json) under the "overlap" key


#### Brief Discussion
1. The findings are discussed inside the jupyter notebook. This is my preferred way of data
exploration. When I want to share insight or discoveries I sent an HTML version to other teams
(engineer and/or product).

2. A brief step by step outline can be found under the docstring (see above link). I have never used
a mapping API outside of basic geocoding.
3. See above
4. See above
5. How to make production ready:
    - Assumptions:
        - Data lives in an environment accessible by job (eg s3, AWS aurora/RDS, or internal)
        - Job passed a dev run and test coverage >=80%
        - CI/CD already setup
    - What is our output:
        - clean coordinate data
        - Mapbox Tileset (geojson)
        - overlap count per coordinate
    - Steps:
        1. Setup AWS Cloudwatch to capture logging that's already in code
        2. Setup dashboard to display job metrics to via AWS kinesis or any stream service (each calculated metric
        can be viewed as an event) 
        3. Setup a CI/CD that builds and deploys to AWS ECS every time the development branch merges into
        master. Deployment step is a two parter: first merge to master and a manual OK to prevent
        accidental merges.
        4. Setup alerting (sucess/fail) via slack or victorOps to DE team and product owner.
        5. Follow product team requests and deliver outputs to database or s3.
        5. And a 4-leaf clover for good measure (jk)

    - Other things to consider:
        1. Team SLA
        2. Does the output serve as input for existing job
        3. Frequency job needs to run (hourly, daily, etc.)




