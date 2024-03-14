const SERVER_ENDPOINT = "http://localhost:8080";
const IMAGE_PREVIEW = document.getElementById("file-preview");

document.getElementById("fileUpload").addEventListener("change", async (event) => {
    try{
        let formData = new FormData();
        formData.append("articleFile", event.target.files[0]);
        const data = await fetch(`${SERVER_ENDPOINT}/`, {body: formData,method: "POST",}).then((res) => res.json());

        alert('file Uploaded!')
    } catch (error) {
        alert(error.message + " hello");
    }
});

document.getElementById("result").addEventListener("click", async (event) => {
    try {
      const data = await fetch(`${SERVER_ENDPOINT}/upload`, {
        method: "GET",
      }).then((response) => {
         return response.text();
      }).then((html) => {document.body.innerHTML = html});
    } catch (error) {
      alert(error.message + " hello");
    }
});