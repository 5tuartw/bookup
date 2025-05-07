function getJobStatus(jobId) {
    fetch('/results/' + jobId)
        .then(response => response.json())
        .then(data => {
            const resultDiv = document.getElementById('result');
            console.log("Received data:", data);
            if (data && data.status === 'finished') {
                let bookResults = data.result["results_per_title"];
                resultDiv.innerHTML = '';
                
                const resultsTable = document.createElement('table');
                resultsTable.style.width = '100%';
                const tableBody = document.createElement('tbody');
                resultsTable.appendChild(tableBody);
                resultDiv.appendChild(resultsTable);

                const confirmButton = document.createElement('button');
                confirmButton.type = 'button';
                confirmButton.id = 'confirmBtn';
                confirmButton.textContent = 'Confirm Selections and Get Details';
                confirmButton.addEventListener('click', submitConfirmedBooks);
                resultDiv.appendChild(confirmButton);

                const jobIdInput = document.getElementById('job_id');
                if (jobIdInput) jobIdInput.remove();

                bookResults.forEach(result =>{
                    const row = document.createElement('tr');

                    const userInputCell = document.createElement('td');
                    userInputCell.style.padding = '10px';
                    userInputCell.style.verticalAlign = 'top';
                    userInputCell.textContent = `"${result.user_title}"`;
                    row.appendChild(userInputCell);

                    const matchesCell = document.createElement('td');
                    matchesCell.style.padding = '10px';
                    matchesCell.style.verticalAlign = 'top';

                    const radioGroupDiv = document.createElement('div');
                    const groupName = `select_${result.user_title.replace(/\s+/g, '_')}`;

                    const noneRadioId = `none_${groupName}`;
                    const noneRadio = document.createElement('input');
                    noneRadio.type = 'radio';
                    noneRadio.name = groupName;
                    noneRadio.value = 'NONE';
                    noneRadio.id = noneRadioId;

                    const noneLabel = document.createElement('label');
                    noneLabel.htmlFor = noneRadioId;
                    noneLabel.textContent = ' None of these / Exclude this title';
                    noneLabel.style.fontWeight = 'bold';

                    radioGroupDiv.appendChild(noneRadio);
                    radioGroupDiv.appendChild(noneLabel);
                    radioGroupDiv.appendChild(document.createElement('br'));                    

                    if (result.possible_matches.length > 0) {
                        const matchesList = document.createElement('ul');
                        matchesList.style.listStyle = 'none';
                        matchesList.style.paddingLeft = '15px';

                        result.possible_matches.forEach((matchInfo, index) => {
                            const listItem = document.createElement('li');
                            const radioBtn = document.createElement('input');
                            const radioId = `match_${index}_${groupName}`;
                            radioBtn.type = 'radio';
                            radioBtn.name = groupName;
                            radioBtn.value = JSON.stringify(matchInfo.match);
                            radioBtn.id = radioId;

                            if (index === 0) {
                                radioBtn.checked = true;
                            }

                            const label = document.createElement('label');
                            label.htmlFor = radioId
                            label.textContent = ` ${matchInfo.match.title} by ${matchInfo.match.authors.join(', ')} (ISBN: ${matchInfo.match.isbn})`;
                            label.prepend(radioBtn);

                            listItem.appendChild(label);
                            matchesList.appendChild(listItem);
                        });
                        radioGroupDiv.appendChild(matchesList);
                    } else {
                        const noMatchPara = document.createElement('p');
                        noMatchPara.textContent = 'No matches found by Google Search';
                        noMatchPara.style.marginLeft = '15px';
                        radioGroupDiv.appendChild(noMatchPara);
                        noneRadio.checked = true;
                    }
                    matchesCell.appendChild(radioGroupDiv);
                    row.appendChild(matchesCell);
                    tableBody.appendChild(row);
                });
                resultDiv.appendChild(confirmButton);
            } else if (data && data.status === 'failed') {
                resultDiv.innerText = 'Book search failed: ' + data.error;
            } else if (data) {
                resultDiv.innerText = 'Book search pending...';
                setTimeout(function() {
                    getJobStatus(jobId);
                }, 1000);
            } else {
                resultDiv.innerText = 'Error fetching results.'
            }
        })
        .catch(error => {
            document.getElementById('result').innerText = 'Error communicating with the server.'
        });
}

function submitConfirmedBooks() {
    const confirmedBooks = [];
    const isbnList = [];
    const radioGroups = document.querySelectorAll('input[type="radio"]');

    const selections = {};
    radioGroups.forEach(radio => {
        if (radio.checked) {
            selections[radio.getAttribute('name')] = radio.value;
        }
    });

    const confirmedBooksDiv = document.getElementById('confirmed_books');
    confirmedBooksDiv.innerHTML = '<h3>Confirmed Books for analysis:</h3><ul></ul>';
    const confirmedList = confirmedBooksDiv.querySelector('ul');

    for (const groupName in selections) {
        const selectedValue = selections[groupName];
        if (selectedValue === 'NONE'){
            console.log(`User excluded selection for group: ${groupName}`);
            continue
        }
        try{
            const book = JSON.parse(selectedValue);
            const listItem = document.createElement('li');
            listItem.textContent = `${book.title} by ${book.authors.join(', ')} (ISBN: ${book.isbn})`;
            confirmedList.appendChild(listItem)
            if (book.isbn) {
                isbnList.push(book.isbn)
            } else {
                console.warn(`Selected book for group ${groupName} missing ISBN:`, book);
            }
        } catch (e) {
            console.error(`Error parsing selected book data for group ${groupName}:`, selectedValue, e);
        }
    }

    console.log("Final ISBN list being sent to /fetch_book_data", isbnList);
    
    if (isbnList.length === 0) {
        alert("No books selected to proceed with analysis. Please select at least one match.");
        document.getElementById('status').innerHTML = 'No books selected.';
        const confirmBtn = document.getElementById('confirmBtn');
        if(confirmBtn) confirmBtn.disabled = false;
        return;
    }

    fetch('/fetch_book_data', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({ isbnList: isbnList})
    })
    .then(response => response.json())
    .then(data => {
        console.log("Book data fetched:", data);
        
        document.getElementById('status').innerHTML = 'Book details received. Starting background analysis...';
        document.getElementById('confirmed_books').style.display = 'block';

        enqueueLLMAnalysis(data);

        const confirmBtn = document.getElementById('confirmBtn');
        if(confirmBtn) confirmBtn.disabled = true;
    })
    .catch(error => {
        console.error("Error fetching book data:", error);
    });
}

document.addEventListener('DOMContentLoaded', function() {
    const jobIdElement = document.getElementById('job_id');
    if (jobIdElement && jobIdElement.value){
        const jobId = jobIdElement.value;
        const resultDiv = document.getElementById('result');
        resultDiv.innerText = 'Book search pending...';
        getJobStatus(jobId);
    }
});

function enqueueLLMAnalysis(detailedBookData) {
    console.log("Sending data to enqueue LLM analysis:", detailedBookData);
    const statusDiv = document.getElementById('status');
    document.getElementById('status').innerHTML = 'Requesting background LLM analysis...';

    fetch('/enqueue_llm_analysis', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(detailedBookData),
    })
    .then(response => {
        if (!response.ok) {
            return response.json().then(err => {
                throw new Error(err.error || `HTTP error ${response.status}`);
            }).catch(() => {
                throw new Error(`HTTP error ${response.status}`);
            });
        }
        return response.json();
    })
    .then(data => {
        if (data.job_id) {
            console.log('LLM analysis job enqueued with ID:', data.job_id);
            if(statusDiv) statusDiv.innerHTML = 'Background LLM analysis started... waiting for results.';
            checkLLMJobStatus(data.job_id);
        } else {
            console.error('Error: No job ID received for LLM task.');
            if(statusDiv) statusDiv.innerHTML = 'Error: Could not get analysis job ID.';
        }
    })
    .catch(error => {
        console.error('Error enqueueing LLM task:', error);
        if(statusDiv) statusDiv.innerHTML = `Error starting analysis: ${error.message}`;
        const confirmBtn = document.getElementById('confirmBtn');
        if(confirmBtn) confirmBtn.disabled = false;
    });
}

function checkLLMJobStatus(jobId) {
    fetch(`/results/${jobId}`)
        .then(response => response.json())
        .then(data => {
            const statusDiv = document.getElementById('status');
            if (!statusDiv) return;

            if (data.status === 'finished') {
                statusDiv.innerHTML = 'Analysis complete!';
                console.log("LLM Analysis results received:", data.result);

                displayFinalResults(data.result);

            } else if (data.status === 'failed') {
                statusDiv.innerHTML = `LLM analysis job failed: ${data.error}`;
                console.error("LLM Job Failed:", data.error);
                const resultDiv = document.getElementById('result');
                if(resultDiv) resultDiv.innerHTML = '<p>Analysis failed. Please try again.</p>';

            } else {
                statusDiv.innerHTML = 'LLM analysis in progress... This may take a minute.';
                setTimeout(() => checkLLMJobStatus(jobId), 5000);
            }
        })
        .catch(error => {
            console.error('Error checking LLM job status:', error);
            const statusDiv = document.getElementById('status');
            if (statusDiv) statusDiv.innerHTML = 'Error checking analysis job status.';
        });
}

function displayFinalResults(finalBookData) {
    const confirmedBooksDiv = document.getElementById('confirmed_books');
    if (confirmedBooksDiv) {
         confirmedBooksDiv.innerHTML = '<h3>Analysed Books:</h3><ul></ul>';
         const listElement = confirmedBooksDiv.querySelector('ul');

         if (!finalBookData || Object.keys(finalBookData).length === 0) {
             listElement.innerHTML = '<li>No analysis results available.</li>';
         } else {
             for (const isbn in finalBookData) {
                 const book = finalBookData[isbn];
                 if (!book || typeof book !== 'object') continue;

                 const listItem = document.createElement('li');
                 let authors = Array.isArray(book.authors) ? book.authors.join(', ') : 'Unknown Author';
                 let sentimentText = book.llm_sentiment ? ` LLM Sentiment: ${book.llm_sentiment}` : ' (LLM Sentiment N/A)';
                 listItem.textContent = `${book.title || 'Unknown Title'} by ${authors} (ISBN: ${isbn})${sentimentText}`;
                 listElement.appendChild(listItem);
             }
         }
         confirmedBooksDiv.style.display = 'block';
    }

    let allThemes = [];
    if (finalBookData) {
        for (const isbn in finalBookData) {
             const book = finalBookData[isbn];
             if (book && book.llm_themes && Array.isArray(book.llm_themes)) {
                 allThemes = allThemes.concat(book.llm_themes.map(t => String(t).toLowerCase()));
             }
        }
    }
    const themeFrequencies = {};
    allThemes.forEach(theme => {
        if(theme) themeFrequencies[theme] = (themeFrequencies[theme] || 0) + 1;
    });
    const sortedThemes = Object.entries(themeFrequencies)
        .filter(([theme, count]) => count > 1)
        .sort(([, countA], [, countB]) => countB - countA)
        .slice(0, 10);

    const themesDiv = document.getElementById('common_keywords');
    if (themesDiv) {
        themesDiv.innerHTML = '<h3>Common Themes (from LLM Analysis):</h3>';
        const list = document.createElement('ul');
        if (sortedThemes.length === 0) {
             list.innerHTML = '<li>No significant common themes found across multiple books.</li>';
        } else {
            sortedThemes.forEach(([theme, count]) => {
                const item = document.createElement('li');
                const displayTheme = theme.charAt(0).toUpperCase() + theme.slice(1);
                item.textContent = `${displayTheme} (Appears for ${count} books)`;
                list.appendChild(item);
            });
        }
        themesDiv.appendChild(list);
        themesDiv.style.display = 'block';
    }

     const resultDiv = document.getElementById('result');
     if(resultDiv) resultDiv.innerHTML = '';
}

/* removed temporarily
function processKeywords(bookData) {
    let allKeywords = []
    for (const isbn in bookData) {
        if (bookData[isbn].keywords && Array.isArray(bookData[isbn].keywords)) {
            const lowerCaseKeywords = bookData[isbn].keywords.map(kw => kw.toLowerCase());
            allKeywords = allKeywords.concat(lowerCaseKeywords);
        }
    }
    
    const keywordFrequencies = {};
    allKeywords.forEach(keyword => {
        if (keyword.length < 3) {
            return;
        }
        keywordFrequencies[keyword] = (keywordFrequencies[keyword] || 0) +1;
    });

    const sortedKeywords = Object.entries(keywordFrequencies)
        .sort(([, countA], [, countB]) => countB - countA);
    
    displayTopKeyWords(sortedKeywords);
}

function displayTopKeyWords(sortedKeywords) {
    const keywordsDiv = document.getElementById('common_keywords');
    if (!keywordsDiv) return;

    keywordsDiv.innerHTML = '<h3>Common Themes/Keywords:</h3>';
    const list = document.createElement('ul');

    const topKeywords = sortedKeywords.filter(([, count]) => count > 1).slice(0, 10);
    if (topKeywords.length === 0) {
        list.innerHTML = '<li> No significant comon keywords found across multiple books</li>';
    } else {
        topKeywords.forEach(([keyword, count]) => {
            const item = document.createElement('li');
            item.textContent = `${keyword} (appears in ${count} book descriptions)`;
            list.appendChild(item)
        });
    }
    keywordsDiv.appendChild(list)
    keywordsDiv.style.display = 'block';
}
*/