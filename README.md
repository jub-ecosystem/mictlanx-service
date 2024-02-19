<p align="center">
  <img width="200" src="./assets/logo.png" />
</p>

<div align=center>
<a href="https://test.pypi.org/project/mictlanx/"><img src="https://img.shields.io/badge/build-0.0.110-2ea44f?logo=Logo&logoColor=%23000" alt="build - 0.0.110"></a>
</div>
<div align=center>
	<h1>MictlanX: <span style="font-weight:normal;"> Router  </span></h1>
</div>

<!-- #  MictlanX  -->
**MictlanX** is a prototype storage system developed for my PhD thesis - titled as *Reactive elastic replication strategy for ephemeral computing*.  For now the source code is kept private, and it is for the exclusive use of the *Muyal-ilal* research group. 


## Getting started 🚀

A conceptual representation is shown in the image below, 1) The user or application which produce and consume. The files can be allocated in the system using the 2)  ```MictlanX - Client```, this python-based program makes easy the communication with the system, 3) ```MictlanX Router``` by now is in working progress, its role is to balance the load across a set of $VSS$ and, 4) The users can also send availability policies that perform some predefine operation over the peers and update its replication strategy. 

<p align="center">
  <img width="500" src="./assets/01.png" />
</p>


## Prerequisites 🧾
You must meet the prerequisites to run successfully the MictlanX Client: 
1.  Installing dependencies using the following command:
	```sh
	pip3 install -r ./requirements.txt
	```
<!-- CONTRIBUTING -->
## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement".
Don't forget to give the project a star! Thanks again!

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- LICENSE -->
## License

Distributed under the MIT License. See `LICENSE.txt` for more information.

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- CONTACT -->
## Contact

 Ignacio Castillo - [@NachoCastillo]() - jesus.castillo.b@cinvestav.mx

<p align="right">(<a href="#top">back to top</a>)</p>
