const storeUrls = {
    '전남대후문점' : 'http://211.107.176.115:8080/login/loginPage?type=seldy',
    '풍암점' : 'http://115.23.30.158:8080/login/loginPage?type=seldy',
    '봉선삼익점' : 'http://125.136.145.182:8080/login/loginPage?type=seldy',
    '나주혁신점' : 'http://183.105.73.68:8080/login/loginPage?type=seldy',
    '수완장덕점' : 'http://121.148.104.49:8080/login/loginPage?type=seldy',
    '광한루점' : 'http://112.184.84.162:8080/login/loginPage?type=seldy',
    '광양중동점' : 'http://211.198.199.101:8080/login/loginPage?type=seldy',
    '상무점' : 'http://112.164.37.51:8080/login/loginPage?type=seldy',
    '군산미장점' : 'http://220.90.170.30:8080/login/loginPage?type=seldy',
};

let selectedStores = [];
let timerInterval;
let estimatedTime = 20;

function toggleSelection(store, button) {
    const index = selectedStores.indexOf(store);
    
    if (index > -1) {
        // 이미 선택된 경우 선택 해제
        selectedStores.splice(index, 1);
        button.classList.remove('selected');
    } else {
        // 선택되지 않은 경우 선택
        selectedStores.push(store);
        button.classList.add('selected');
    }
}

function submitForm() {
    const username = document.getElementById('username').value;
    const password = document.getElementById('password').value;
    const startDate = document.getElementById('start-date').value;
    const endDate = document.getElementById('end-date').value;

    if (selectedStores.length === 0 || !startDate || !endDate) {
        alert('지점과 기간을 모두 선택해 주세요.');
        return;
    }

    let elapsedTime = 0;
    document.getElementById('timer').innerText = `경과 시간 : ${elapsedTime}초\n예상 시간 : ${estimatedTime}초`;

    // 타이머 업데이트
    timerInterval = setInterval(() => {
        elapsedTime++;
        document.getElementById('timer').innerText = `경과 시간 : ${elapsedTime}초\n예상 시간 : ${estimatedTime}분`;
    }, 1000);

    const urls = selectedStores.map(store => storeUrls[store]);

    fetch('http://127.0.0.1:8000/home/getdata/', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            // 'X-CSRFToken': getCSRFToken()
        },
        body: JSON.stringify({
            username: username,
            password: password,
            stores: selectedStores,
            urls: urls,
            start_date: startDate,
            end_date: endDate
        })
    })
    .then(response => {
        if (!response.ok) {
            throw new Error('Error while processing');
        }
        return response.blob();
    })
    .then(blob => {
        const link = document.createElement('a');
        const downloadUrl = window.URL.createObjectURL(blob);
        link.href = downloadUrl;
        link.setAttribute('download', '결제정보.zip');  // 파일 이름 설정
        document.body.appendChild(link);
        link.click(); // 다운로드 실행
        link.parentNode.removeChild(link); // 링크 제거
        clearInterval(timerInterval);  // 타이머 중지
        // alert('Final file created successfully')
    })
    .catch(error => {
        console.error('Error :', error);
        alert('크롤링 중 오류 발생');
        clearInterval(timerInterval);  // 타이머 중지
    });
}

// function getCSRFToken() {
//     const cookieValue = documnet.cookie.split('; ')
//         .find(row => row.startsWith('csrftoken'))
//         ?.split('=')[1];
//     return cookieValue || '';
// }