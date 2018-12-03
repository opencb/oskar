# Overview
Oskar is an open-source project that aims to implement a framework for genomic scale _big data_ analysis of hundreds of terabytes or even petabytes. All these can be queried through a command line interface or be used as a library.

Oskar constitutes the big data analysis component of [OpenCB](http://www.opencb.org/) initiative. 

### Documentation
You can find Oskar documentation and tutorials at: http://docs.opencb.org/display/oskar.

### Issues Tracking
You can report bugs or request new features at [GitHub issue tracking](https://github.com/opencb/oskar/issues).

### Release Notes and Roadmap
Releases notes are available at [GitHub releases](https://github.com/opencb/oskar/releases).

Roadmap is available at [GitHub milestones](https://github.com/opencb/oskar/milestones). You can report bugs or request new features at [GitHub issue tracking](https://github.com/opencb/oskar/issues).

### Versioning
Oskar is versioned following the rules from [Semantic versioning](http://semver.org/).

### Maintainers
We recommend to contact Oskar developers by writing to OpenCB mailing list opencb@googlegroups.com. Current main developers and maintainers are:
* Ignacio Medina (im411@cam.ac.uk) (_Founder and Project Leader_)
* Jacobo Coll (jacobo.coll-moragon@genomicsengland.co.uk)
* Joaquin Tarraga (jt645@cam.ac.uk)

##### Contributing
Oskar is an open-source and collaborative project. We appreciate any help and feedback from users, you can contribute in many different ways such as simple bug reporting and feature request. Dependending on your skills you are more than welcome to develop client tools, new features or even fixing bugs.


# How to build 
Oskar is mainly developed in Java and it uses [Apache Maven](http://maven.apache.org/) as building tool. Oskar requires Java 8, in particular **JDK 1.8.0_60+**, and other OpenCB dependencies that can be found in [Maven Central Repository](http://search.maven.org/).

Stable releases are merged and tagged at **_master_** branch, you are encourage to use latest stable release for production. Current active development is carried out at **_develop_** branch and need **Java 8**, in particular **JDK 1.8.0_60+**, only compilation is guaranteed and bugs are expected, use this branch for development or for testing new functionalities. Dependencies of **_master_** branch are ensured to be deployed at [Maven Central Repository](http://search.maven.org/), but dependencies for **_develop_** branch may require users to download and install the following git repositories from OpenCB:
* _java-common-libs_: https://github.com/opencb/java-common-libs (branch 'develop')
* _biodata_: https://github.com/opencb/biodata (branch 'develop')
* _cellbase_: https://github.com/opencb/cellbase (branch 'develop')

### Cloning
Oskar is an open-source and free project, you can download default **_develop_** branch by executing:

    imedina@ivory:~$ git clone https://github.com/opencb/oskar.git
    Cloning into 'oskar'...
    remote: Counting objects: 20267, done.
    remote: Compressing objects: 100% (219/219), done.
    remote: Total 20267 (delta 105), reused 229 (delta 35)
    Receiving objects: 100% (20267/20267), 7.23 MiB | 944.00 KiB/s, done.
    Resolving deltas: 100% (6363/6363), done.

Latest stable release at **_master_** branch can be downloaded executing:

    imedina@ivory:~$ git clone -b master https://github.com/opencb/oskar.git
    Cloning into 'oskar'...
    remote: Counting objects: 20267, done.
    remote: Compressing objects: 100% (219/219), done.
    remote: Total 20267 (delta 105), reused 229 (delta 35)
    Receiving objects: 100% (20267/20267), 7.23 MiB | 812.00 KiB/s, done.
    Resolving deltas: 100% (6363/6363), done.


### Build
You can build Oskar by executing the following command from the root of the cloned repository:
  
    $ mvn clean install -DskipTests

Remember that **_develop_** branch dependencies are not ensured to be deployed at Maven Central, you may need to clone and install **_develop_** branches from OpenCB _biodata_, _datastore_, _cellbase_ and _hpg-bigdata_ repositories. After this you should have this file structure in **_Oskar/build_**:

    oskar/build/
    ├── bin/
    ├── conf/
    ├── libs/
    ├── LICENSE
    ├── README.md

You can copy the content of the _build_ folder into the installation directory such as _/opt/Oskar_.

### Testing
You can run the unit tests using Maven or your favorite IDE. Just notice that some tests may require of certain database back-ends such as MongoDB or Apache HBase and may fail if they are not available.

### Command Line Interface (CLI)
If the build process has gone well you should get an integrated help by executing:

    ./bin/oskar.sh --help

You can find more detailed documentation and tutorials at http://docs.opencb.org/display/oskar.

### Other Dependencies
We try to improve the admin experience by making the installation and build as simple as possible. Unfortunately, for some Oskar components and functionalities other dependencies are required.

##### Spark
At this moment [Spark](http://spark.apache.org/) is used for running most of the as _big data_ variant analysis.