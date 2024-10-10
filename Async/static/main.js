// custom javascript
const stringToColour = function (str) {
  var hash = 0;
  for (var i = 0; i < str.length; i++) {
    hash = str.charCodeAt(i) + ((hash << 5) - hash);
  }
  var colour = '#';
  for (var i = 0; i < 3; i++) {
    var value = (hash >> (i * 8)) & 0xFF;
    colour += ('00' + value.toString(16)).substr(-2);
  }
  return colour;
};

(function () {
  console.log('Sanity Check!');
})();

var count = 0;
function handleClick(type, val, social) {
  console.log(social)
  fetch(`/${social}/${val.replace("#","%23")}`, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json'
    }
  })
    .then(response => response.json())
    .then(data => getStatus(data.campaign_id));
}

function handleCampaign(type, val, action) {
  console.log(val, action)
  if (action == 'status') {
    fetch(`/campaign/${val}`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json'
      }
    }).then(res => res.json()).then(res => document.getElementById('campaign_result').innerText = JSON.stringify(res, null, 2))
  }
  if(action == 'stop'){
    fetch(`/campaign/${val}/${action}`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json'
      }
    }).then(res => res.json()).then(res => document.getElementById('campaign_result').innerText = JSON.stringify(res, null, 2))
  }
  if (action == 'restart') {
    fetch(`/campaign/${val}/${action}`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json'
      }
    }).then(res => res.json()).then(res => { document.getElementById('campaign_result').innerText = JSON.stringify(res, null, 2); getStatus(res.campaign_id) })
  }

}

function getStatus(taskID) {
  fetch(`/campaign/${taskID}`, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json'
    },
  })
    .then(response => response.json())
    .then(res => {
      const date = new Date()
      const string = `${date.getDate()}/${date.getMonth()}/${date.getFullYear()} ${date.getHours()}:${date.getMinutes()}:${date.getSeconds()}`
      const taskStatus = res.state;

      const html = `
      <tr>
        <td style="color: ${stringToColour(res.id)}; cursor:pointer" onclick="document.getElementById('campaign').value = '${res.id}'">${res.id}</td>
        <td>${res.state}</td>
        <td><a href="${res.result}">${res.result}</a></td>
        <td>${string}</td>

      </tr>`;
      const newRow = document.getElementById('tasks').insertRow(0);
      newRow.innerHTML = html;


      if (taskStatus === 'SUCCESS' || taskStatus === 'FAILURE' || taskStatus === 'REVOKED') {
        count++;
        return false;
      }
      setTimeout(function () {
        getStatus(res.id);
      }, 2000);
    })
    .catch(err => console.log(err));
}
