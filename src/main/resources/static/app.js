// State Management
const state = {
    currentTab: 'dashboard',
    keys: [],
    stats: {},
    commandHistory: [],
    historyIndex: -1,
    refreshInterval: null
};

// DOM Elements
const elements = {
    navItems: document.querySelectorAll('.nav-item'),
    tabContents: document.querySelectorAll('.tab-content'),
    pageTitle: document.getElementById('page-title'),
    roleBadge: document.getElementById('role-badge'),
    sidebarStatus: document.getElementById('sidebar-status'),
    statusIndicator: document.querySelector('.status-indicator'),
    
    // Stats elements
    uptimeVal: document.getElementById('uptime-val'),
    keysCountVal: document.getElementById('keys-count-val'),
    replicasCountVal: document.getElementById('replicas-count-val'),
    memoryUsageVal: document.getElementById('memory-usage-val'),
    memoryRatio: document.getElementById('memory-ratio'),
    memoryProgressFill: document.getElementById('memory-progress-fill'),
    memoryUsedRaw: document.getElementById('memory-used-raw'),
    memoryMaxRaw: document.getElementById('memory-max-raw'),
    jvmVersionVal: document.getElementById('jvm-version-val'),
    supportedCommandsList: document.getElementById('supported-commands-list'),
    
    // Keys Browser elements
    keySearch: document.getElementById('key-search'),
    refreshKeysBtn: document.getElementById('refresh-keys-btn'),
    keysTableBody: document.getElementById('keys-table-body'),
    addKeyModalBtn: document.getElementById('add-key-modal-btn'),
    
    // Terminal elements
    consoleOutput: document.getElementById('console-output'),
    consoleInput: document.getElementById('console-input'),
    clearConsoleBtn: document.getElementById('clear-console-btn'),
    
    // Modal elements
    addKeyModal: document.getElementById('add-key-modal'),
    closeModalBtn: document.getElementById('close-modal-btn'),
    cancelKeyBtn: document.getElementById('cancel-key-btn'),
    saveKeyBtn: document.getElementById('save-key-btn'),
    newKeyName: document.getElementById('new-key-name'),
    newKeyValue: document.getElementById('new-key-value'),
    newKeyExpiry: document.getElementById('new-key-expiry')
};

// Initialize Application
document.addEventListener('DOMContentLoaded', () => {
    setupTabNavigation();
    setupTerminal();
    setupKeyBrowser();
    setupModal();
    
    // Initial fetch
    fetchStats();
    fetchKeys();
    fetchInfo(); // For supported commands list
    
    // Periodically poll stats and keys
    state.refreshInterval = setInterval(() => {
        fetchStats();
        if (state.currentTab === 'keys') {
            fetchKeys();
        }
    }, 2000);
});

// Tab Navigation logic
function setupTabNavigation() {
    elements.navItems.forEach(item => {
        item.addEventListener('click', (e) => {
            e.preventDefault();
            const tabId = item.getAttribute('data-tab');
            switchTab(tabId);
        });
    });
}

function switchTab(tabId) {
    state.currentTab = tabId;
    
    // Update active class on nav
    elements.navItems.forEach(item => {
        if (item.getAttribute('data-tab') === tabId) {
            item.classList.add('active');
        } else {
            item.classList.remove('active');
        }
    });
    
    // Update active class on content
    elements.tabContents.forEach(content => {
        if (content.id === `tab-${tabId}`) {
            content.classList.add('active');
        } else {
            content.classList.remove('active');
        }
    });
    
    // Update page title
    const titles = {
        dashboard: 'Overview',
        keys: 'Keys Browser',
        console: 'Terminal Console'
    };
    elements.pageTitle.textContent = titles[tabId] || 'Overview';
    
    if (tabId === 'keys') {
        fetchKeys();
    }
}

// API Communication Helper
async function apiCall(endpoint, method = 'GET', body = null) {
    try {
        const options = {
            method,
            headers: {
                'Content-Type': 'application/json'
            }
        };
        if (body) {
            options.body = JSON.stringify(body);
        }
        
        const response = await fetch(endpoint, options);
        
        // If not successful
        if (!response.ok) {
            const errData = await response.json().catch(() => ({}));
            throw new Error(errData.error || `HTTP error! status: ${response.status}`);
        }
        
        updateConnectionStatus(true);
        return await response.json();
    } catch (error) {
        console.error(`API Call failed to ${endpoint}:`, error);
        updateConnectionStatus(false);
        throw error;
    }
}

function updateConnectionStatus(isConnected) {
    if (isConnected) {
        elements.sidebarStatus.textContent = 'Connected';
        elements.statusIndicator.className = 'status-indicator online';
    } else {
        elements.sidebarStatus.textContent = 'Connection Lost';
        elements.statusIndicator.className = 'status-indicator offline';
    }
}

// Fetch general stats
async function fetchStats() {
    try {
        const stats = await apiCall('/api/stats');
        state.stats = stats;
        
        // Update stats dashboard
        elements.uptimeVal.textContent = formatUptime(stats.uptime);
        elements.keysCountVal.textContent = stats.keys;
        elements.replicasCountVal.textContent = stats.replicas;
        
        // Role badge update
        elements.roleBadge.textContent = stats.role;
        elements.roleBadge.className = `badge role-badge ${stats.role.toLowerCase()}`;
        
        // Memory metrics
        const usedMB = (stats.memory.used / 1024 / 1024).toFixed(1);
        const maxMB = (stats.memory.max / 1024 / 1024).toFixed(1);
        elements.memoryUsageVal.textContent = `${usedMB} MB`;
        elements.memoryUsedRaw.textContent = `Used: ${usedMB} MB`;
        elements.memoryMaxRaw.textContent = `Max: ${maxMB} MB`;
        
        const ratio = ((stats.memory.used / stats.memory.max) * 100).toFixed(1);
        elements.memoryRatio.textContent = `${ratio}%`;
        elements.memoryProgressFill.style.width = `${ratio}%`;
        
        elements.jvmVersionVal.textContent = stats.javaVersion;
    } catch (e) {
        // Handled in apiCall
    }
}

// Fetch supported commands
async function fetchInfo() {
    try {
        const info = await apiCall('/api/info');
        
        // Render commands tags
        elements.supportedCommandsList.innerHTML = '';
        if (info.commands && info.commands.length > 0) {
            info.commands.forEach(cmd => {
                const tag = document.createElement('span');
                tag.className = 'cmd-tag';
                tag.textContent = cmd;
                elements.supportedCommandsList.appendChild(tag);
            });
        } else {
            elements.supportedCommandsList.innerHTML = '<span class="text-muted">No commands returned.</span>';
        }
    } catch (e) {}
}

// Format uptime (seconds to hh:mm:ss)
function formatUptime(sec) {
    const hours = Math.floor(sec / 3600);
    const minutes = Math.floor((sec % 3600) / 60);
    const seconds = sec % 60;
    
    return [
        hours.toString().padStart(2, '0'),
        minutes.toString().padStart(2, '0'),
        seconds.toString().padStart(2, '0')
    ].join(':');
}

// Keys Browser logic
function setupKeyBrowser() {
    elements.refreshKeysBtn.addEventListener('click', fetchKeys);
    
    elements.keySearch.addEventListener('input', () => {
        renderKeysTable();
    });
}

async function fetchKeys() {
    try {
        const keys = await apiCall('/api/keys');
        state.keys = keys;
        renderKeysTable();
    } catch (e) {}
}

function renderKeysTable() {
    const searchTerm = elements.keySearch.value.trim().toLowerCase();
    const filteredKeys = state.keys.filter(k => 
        k.key.toLowerCase().includes(searchTerm)
    );
    
    elements.keysTableBody.innerHTML = '';
    
    if (filteredKeys.length === 0) {
        elements.keysTableBody.innerHTML = `
            <tr>
                <td colspan="4" class="no-data">
                    ${searchTerm ? 'No keys match your search.' : 'Database is empty.'}
                </td>
            </tr>`;
        return;
    }
    
    filteredKeys.forEach(k => {
        const row = document.createElement('tr');
        
        // Key cell
        const keyCell = document.createElement('td');
        keyCell.className = 'font-semibold';
        keyCell.textContent = k.key;
        
        // Type cell
        const typeCell = document.createElement('td');
        const badge = document.createElement('span');
        badge.className = `key-badge ${k.type}`;
        badge.textContent = k.type.toUpperCase();
        typeCell.appendChild(badge);
        
        // Value cell
        const valueCell = document.createElement('td');
        valueCell.className = 'font-mono text-muted text-sm';
        valueCell.textContent = truncateString(k.value, 40);
        
        // Actions cell
        const actionsCell = document.createElement('td');
        const delBtn = document.createElement('button');
        delBtn.className = 'btn-delete';
        delBtn.innerHTML = `
            <svg class="btn-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <polyline points="3 6 5 6 21 6"/>
                <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/>
                <line x1="10" y1="11" x2="10" y2="17"/>
                <line x1="14" y1="11" x2="14" y2="17"/>
            </svg>`;
        delBtn.addEventListener('click', () => deleteKey(k.key));
        actionsCell.appendChild(delBtn);
        
        row.appendChild(keyCell);
        row.appendChild(typeCell);
        row.appendChild(valueCell);
        row.appendChild(actionsCell);
        
        elements.keysTableBody.appendChild(row);
    });
}

function truncateString(str, len) {
    if (str.length <= len) return str;
    return str.substring(0, len) + '...';
}

async function deleteKey(key) {
    if (confirm(`Are you sure you want to delete key "${key}"?`)) {
        try {
            const resp = await apiCall('/api/command', 'POST', { command: `DEL ${key}` });
            if (resp.result && resp.result.startsWith(':1')) {
                // Key deleted
                fetchKeys();
                fetchStats();
            } else {
                alert(`Delete response: ${resp.result}`);
            }
        } catch (e) {
            alert(`Failed to delete key: ${e.message}`);
        }
    }
}

// Terminal Console logic
function setupTerminal() {
    elements.clearConsoleBtn.addEventListener('click', () => {
        elements.consoleOutput.innerHTML = `
            <div class="console-line system">Terminal cleared.</div>`;
    });
    
    elements.consoleInput.addEventListener('keydown', async (e) => {
        if (e.key === 'Enter') {
            const command = elements.consoleInput.value.trim();
            if (!command) return;
            
            elements.consoleInput.value = '';
            appendConsoleLine(command, 'input');
            
            // Add to history
            state.commandHistory.push(command);
            state.historyIndex = state.commandHistory.length;
            
            // Execute command
            try {
                const resp = await apiCall('/api/command', 'POST', { command });
                if (resp.result !== undefined) {
                    appendConsoleLine(resp.result, 'output-success');
                } else if (resp.error) {
                    appendConsoleLine(resp.error, 'output-error');
                }
            } catch (err) {
                appendConsoleLine(err.message, 'output-error');
            }
            
            // Scroll to bottom
            elements.consoleOutput.scrollTop = elements.consoleOutput.scrollHeight;
        } else if (e.key === 'ArrowUp') {
            e.preventDefault();
            if (state.historyIndex > 0) {
                state.historyIndex--;
                elements.consoleInput.value = state.commandHistory[state.historyIndex];
            }
        } else if (e.key === 'ArrowDown') {
            e.preventDefault();
            if (state.historyIndex < state.commandHistory.length - 1) {
                state.historyIndex++;
                elements.consoleInput.value = state.commandHistory[state.historyIndex];
            } else {
                state.historyIndex = state.commandHistory.length;
                elements.consoleInput.value = '';
            }
        }
    });
}

function appendConsoleLine(text, type) {
    const line = document.createElement('div');
    line.className = `console-line ${type}`;
    line.textContent = text;
    elements.consoleOutput.appendChild(line);
}

// Modal Form logic
function setupModal() {
    elements.addKeyModalBtn.addEventListener('click', () => {
        elements.addKeyModal.classList.add('active');
        elements.newKeyName.focus();
    });
    
    const closeModal = () => {
        elements.addKeyModal.classList.remove('active');
        // Clear fields
        elements.newKeyName.value = '';
        elements.newKeyValue.value = '';
        elements.newKeyExpiry.value = '';
    };
    
    elements.closeModalBtn.addEventListener('click', closeModal);
    elements.cancelKeyBtn.addEventListener('click', closeModal);
    
    // Close modal when clicking outside
    elements.addKeyModal.addEventListener('click', (e) => {
        if (e.target === elements.addKeyModal) {
            closeModal();
        }
    });
    
    elements.saveKeyBtn.addEventListener('click', async () => {
        const key = elements.newKeyName.value.trim();
        const val = elements.newKeyValue.value.trim();
        const expiry = elements.newKeyExpiry.value.trim();
        
        if (!key || !val) {
            alert('Both Key and Value are required fields.');
            return;
        }
        
        let cmd = `SET ${key} ${val}`;
        if (expiry) {
            cmd += ` EX ${expiry}`;
        }
        
        try {
            const resp = await apiCall('/api/command', 'POST', { command: cmd });
            if (resp.result === '+OK') {
                closeModal();
                fetchKeys();
                fetchStats();
            } else {
                alert(`Error saving key: ${resp.result}`);
            }
        } catch (e) {
            alert(`Error saving key: ${e.message}`);
        }
    });
}
