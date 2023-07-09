import * as L from 'leaflet';

const state = ["normal", "selecting"]
let map = L.map('map').setView([38, -99], 4);
map.doubleClickZoom.disable();
let curState = 0;
const b = document.getElementById("draw");
if(b) {
  b.onclick = drawButtonHandle;
}
let layers = L.layerGroup().addTo(map);
let ps = L.polygon([]).addTo(map);
// let pts: Array<L.LatLngTuple> = [];



L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
    attribution: '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>'
}).addTo(map);

export function drawButtonHandle() {
  switch (state[curState]) {
    case "normal":
      curState++;
      if(b) {
        b.textContent = "cancel";
      };
      return
    case "selecting":
      ps.setLatLngs([]);
      curState = (curState + 1)%state.length;
      if(b) {
        b.textContent = "Draw";
      }
      return
  };
      
};
function clickHandle(ev: L.LeafletMouseEvent) {
  switch(state[curState]) {
    case "selecting":
      console.log(ev.latlng);
      ps.addLatLng(ev.latlng);
  };
}
type popResponse = {
  population: number
}
async function postData(jsonstr: string): Promise<[popResponse,popResponse]> {
  console.log(jsonstr);
  let bgRes = await fetch("/polygon/block_group/pop", {
    method:"POST",
    body: jsonstr,
    headers: {
      "Accept": "Application/json",
      "Content-Type": "Application/json"
    }
  });
  let j = await bgRes.json();
  let blRes = await fetch("/polygon/block/pop", {
    method:"POST",
    body: jsonstr,
    headers: {
      "Accept": "Application/json",
      "Content-Type": "Application/json"
    }
  });
  let j2 = await blRes.json()
  return [j, j2];
}

function dblHandle(_: L.LeafletMouseEvent) {
  switch(state[curState]) {
    case "selecting":
      const gjson = ps.toGeoJSON();
      const pop = postData(JSON.stringify(gjson.geometry));
      const center = ps.getBounds().getCenter();
      pop.then((p) => {
        L.popup({
          autoClose: false,
          closeOnClick: false
        })
          .setLatLng(center)
          .setContent(JSON.stringify(p))
          .addTo(map)
          .openPopup(center);
      });

      ps.addTo(layers);
      ps = L.polygon([]).addTo(map);
      curState = (curState + 1)%state.length;
      if(b) {
        b.textContent = "Draw";
      }
  }
}

map.on("click", clickHandle);
map.on("dblclick", dblHandle);

