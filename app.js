// Firebase SDK のインポート
import { initializeApp } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-app.js";
import {
  getAuth,
  signInAnonymously,
  onAuthStateChanged
} from "https://www.gstatic.com/firebasejs/11.6.1/firebase-auth.js";
import {
  getFirestore,
  doc,
  getDoc,
  setDoc,
  collection,
  onSnapshot,
  writeBatch,
  query,
  setLogLevel,
  getDocs // getDocs をインポート
} from "https://www.gstatic.com/firebasejs/11.6.1/firebase-firestore.js";

// --- グローバル変数・定数 ---
let db, auth, userId, appId, correctPasswordHash; // correctPasswordHash は平文パスワードを保持する変数名として流用
let globalDailyStats = {}; // 日別データをグローバルに保持
let currentSummaryPeriod = 'all'; // 現在のサマリー期間 ('all', 'monthly', 'weekly')

// --- DOM要素 (認証画面) ---
// 認証画面の要素は起動時にすぐ取得
const passwordContainer = document.getElementById('password-container');
const mainContent = document.getElementById('main-content');
const passwordForm = document.getElementById('password-form');
const passwordInput = document.getElementById('password-input');
const errorMessage = document.getElementById('error-message');
const showPasswordToggle = document.getElementById('show-password-toggle');
const loginButton = document.getElementById('login-button');
const loginButtonText = document.getElementById('login-button-text');
const loginButtonSpinner = document.getElementById('login-button-spinner');
const passwordLoadingMessage = document.getElementById('password-loading-message');

// --- DOM要素 (メインコンテンツ) ---
// メインコンテンツの要素はログイン後に取得する
let companyGroupSelector, newCompanyGroupInput, addCompanyGroupButton, companyGroupError,
  castSelector, newCastNameInput, addCastButton,
  castError, castLoadingMessage, uploadSection, fileInput, searchSection,
  searchInput, loadingIndicator, resultsContainer,
  dailyDetailsModal, modalTitle, modalBody,
  rangeStartDateInput, rangeEndDateInput, rangeSummaryButton,
  rangeSummaryModal, rangeModalTitle, rangeModalBody,
  summaryAllButton, summaryMonthlyButton, summaryWeeklyButton;

/**
 * Cookieを設定します。
 */
function setCookie(name, value, years) {
  let expires = "";
  if (years) {
    const date = new Date();
    date.setTime(date.getTime() + (years * 365 * 24 * 60 * 60 * 1000));
    expires = "; expires=" + date.toUTCString();
  }
  document.cookie = name + "=" + (value || "") + expires + "; path=/; SameSite=Lax";
}

/**
 * Cookieを取得します。
 */
function getCookie(name) {
  const nameEQ = name + "=";
  const ca = document.cookie.split(';');
  for (let i = 0; i < ca.length; i++) {
    let c = ca[i];
    while (c.charAt(0) == ' ') c = c.substring(1, c.length);
    if (c.indexOf(nameEQ) == 0) return c.substring(nameEQ.length, c.length);
  }
  return null;
}

/**
 * メインコンテンツを表示し、アプリの初期化（イベントリスナー設定）を行います。
 */
function showMainContentAndInitApp() {
  passwordContainer.classList.add('hidden');
  mainContent.classList.remove('hidden');

  // メインコンテンツのDOM要素を取得
  companyGroupSelector = document.getElementById('companyGroupSelector');
  newCompanyGroupInput = document.getElementById('newCompanyGroupInput');
  addCompanyGroupButton = document.getElementById('addCompanyGroupButton');
  companyGroupError = document.getElementById('companyGroupError');
  castSelector = document.getElementById('castSelector');
  newCastNameInput = document.getElementById('newCastNameInput');
  addCastButton = document.getElementById('addCastButton');
  castError = document.getElementById('castError');
  castLoadingMessage = document.getElementById('cast-loading-message');
  uploadSection = document.getElementById('upload-section');
  fileInput = document.getElementById('csvFileInput');
  searchSection = document.getElementById('search-section');
  searchInput = document.getElementById('searchInput');
  loadingIndicator = document.getElementById('loadingIndicator');
  resultsContainer = document.getElementById('resultsContainer');
  dailyDetailsModal = document.getElementById('dailyDetailsModal');
  modalTitle = document.getElementById('modalTitle');
  modalBody = document.getElementById('modalBody');
  rangeStartDateInput = document.getElementById('rangeStartDate');
  rangeEndDateInput = document.getElementById('rangeEndDate');
  rangeSummaryButton = document.getElementById('rangeSummaryButton');
  rangeSummaryModal = document.getElementById('rangeSummaryModal');
  rangeModalTitle = document.getElementById('rangeModalTitle');
  rangeModalBody = document.getElementById('rangeModalBody');
  summaryAllButton = document.getElementById('summaryAllButton');
  summaryMonthlyButton = document.getElementById('summaryMonthlyButton');
  summaryWeeklyButton = document.getElementById('summaryWeeklyButton');

  // メインコンテンツのイベントリスナーを設定
  setupEventListeners();
}

/**
 * パスワード認証フォームを有効化します。
 */
function enablePasswordForm() {
  passwordInput.disabled = false;
  showPasswordToggle.disabled = false;
  loginButton.disabled = false;
  passwordLoadingMessage.textContent = '認証準備完了';
  passwordLoadingMessage.classList.remove('text-gray-500');
  passwordLoadingMessage.classList.add('text-green-600');
}

// --- メインアプリロジック ---

/**
 * アプリのメイン初期化処理
 */
async function initializeMainApp() {
  // 認証画面のDOM要素を取得 (これはFirebase初期化より先に実行)
  // (グローバルスコープで既に取得済み)

  // 1. Firebase設定
  try {
    // ▼▼▼ 変更: 以下の { ... } の中身を、
    // Firebaseコンソールからコピーした「apiKey: "...",」などの
    // 設定値で「置き換え」てください。
    // 「const firebaseConfig =」や「};」を二重に貼り付けないでください。
    const firebaseConfig = {
      apiKey: "AIzaSyDDz9cs9Wgx8Npjrh7FwUB4kF1h8Zwsiik",
      authDomain: "fantia-csv.firebaseapp.com",
      projectId: "fantia-csv",
      storageBucket: "fantia-csv.firebasestorage.app",
      messagingSenderId: "457081920405",
      appId: "1:457081920405:web:6a33cb7f82e1ff7739f49c"
      // measurementId: "G-W1XS2E6VZ9" // このアプリでは不要なため削除
    };
    // ▲▲▲ 変更 ▲▲▲

    // ▼▼▼ 変更: appIdを固定の文字列に設定 ▼▼▼
    // (セキュリティルール /artifacts/ここの文字列/public/... と一致させる)
    appId = 'fantia-analyzer-app';

    // 2. Firebase初期化
    const app = initializeApp(firebaseConfig);
    auth = getAuth(app);
    db = getFirestore(app);
    setLogLevel('Debug'); // デバッグ用

    // 3. 認証状態の監視を先に設定
    onAuthStateChanged(auth, async (user) => {
      if (user) {
        // --- 認証成功時 (匿名 or ログイン済み) ---
        userId = user.uid;
        console.log("Firebase 認証成功. UserID:", userId);

        // 4. DBからパスワード（平文）を取得
        try {
          const passDocRef = doc(db, `artifacts/${appId}/public/data/config/password`);
          const passDocSnap = await getDoc(passDocRef);

          if (passDocSnap.exists()) {
            // データベースに保存されている平文のパスワードを取得
            // フィールド名が 'hash' のままでも中身が平文ならOK
            correctPasswordHash = passDocSnap.data().hash;
            console.log("パスワードの取得成功。");

            // 5. パスワード認証処理
            // 認証済みかCookieでチェック（Cookieの値も平文パスワード）
            if (getCookie('auth_token_lottery_analyzer') === correctPasswordHash) {
              showMainContentAndInitApp();
              loadCompanyGroups(); // メインアプリの会社グループ読み込み開始
            } else {
              // 認証フォームを有効化
              enablePasswordForm();
            }
          } else {
            // パスワードがDBに設定されていない
            console.error("Firestoreにパスワードが設定されていません。");
            passwordLoadingMessage.textContent = 'エラー: 管理者が未設定です。';
            passwordLoadingMessage.classList.add('text-red-500');
          }
        } catch (err) {
          console.error("パスワードの取得に失敗:", err);
          passwordLoadingMessage.textContent = 'エラー: DB接続に失敗しました。';
          passwordLoadingMessage.classList.add('text-red-500');
        }

        // 6. パスワード認証フォームのイベントリスナー設定 (認証状態に関わらず設定)
        setupPasswordFormListeners();

      } else {
        // --- 未認証時 ---
        console.log("未認証状態。匿名サインインを実行します...");
        try {
          await signInAnonymously(auth);
          // 成功すると onAuthStateChanged が再度(userありで)呼ばれる
        } catch (anonError) {
          console.error("匿名サインインに失敗:", anonError);
          passwordLoadingMessage.textContent = 'エラー: 認証サーバに接続できません。';
          passwordLoadingMessage.classList.add('text-red-500');
        }
      }
    });

  } catch (error) {
    console.error("Firebase 初期化エラー:", error);
    passwordLoadingMessage.textContent = 'エラー: 初期化に失敗しました。';
    passwordLoadingMessage.classList.add('text-red-500');
  }
}

/**
 * パスワード認証フォームのイベントリスナーを設定します。
 */
function setupPasswordFormListeners() {
  // パスワードフォームの送信イベント
  passwordForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    loginButton.disabled = true;
    loginButtonText.classList.add('hidden');
    loginButtonSpinner.classList.remove('hidden');

    const inputPassword = passwordInput.value;
    // ハッシュ化の処理を削除

    // デバッグ用ログ
    console.log("入力されたパスワード:", inputPassword);
    console.log("期待されるパスワード:", correctPasswordHash);

    // 平文のまま比較
    if (inputPassword === correctPasswordHash) {
      // 認証成功
      setCookie('auth_token_lottery_analyzer', inputPassword, 10); // Cookieにも平文を保存
      showMainContentAndInitApp();
      loadCompanyGroups(); // メインアプリの会社グループ読み込み開始
    } else {
      // 認証失敗
      errorMessage.classList.remove('hidden');
      passwordInput.value = '';
      loginButton.disabled = false;
      loginButtonText.classList.remove('hidden');
      loginButtonSpinner.classList.add('hidden');
    }
  });

  // パスワード入力欄で入力を開始したらエラーメッセージを消す
  passwordInput.addEventListener('input', () => {
    errorMessage.classList.add('hidden');
  });

  // 「パスワードを表示する」チェックボックスの処理
  if (showPasswordToggle) {
    showPasswordToggle.addEventListener('change', () => {
      passwordInput.type = showPasswordToggle.checked ? 'text' : 'password';
    });
  }
}

/**
 * メインアプリのイベントリスナーを設定します。
 * (showMainContentAndInitAppから呼び出される)
 */
function setupEventListeners() {
  // 会社グループ選択
  companyGroupSelector.addEventListener('change', (e) => {
    const companyGroupId = e.target.value;
    if (companyGroupId) {
      loadCastsForCompanyGroup(companyGroupId);
      // サマリーボタンを有効化
      summaryAllButton.disabled = false;
      summaryMonthlyButton.disabled = false;
      summaryWeeklyButton.disabled = false;
    } else {
      castSelector.innerHTML = '<option value="">会社グループを選択してください</option>';
      castSelector.disabled = true;
      uploadSection.classList.add('hidden');
      resultsContainer.innerHTML = '';
      searchSection.classList.add('hidden');
      // サマリーボタンを無効化
      summaryAllButton.disabled = true;
      summaryMonthlyButton.disabled = true;
      summaryWeeklyButton.disabled = true;
    }
  });

  // 会社グループ追加ボタン
  addCompanyGroupButton.addEventListener('click', handleAddCompanyGroup);

  // 会社グループ追加Enterキー
  newCompanyGroupInput.addEventListener('keypress', (e) => {
    if (e.key === 'Enter') {
      handleAddCompanyGroup();
    }
  });

  // キャスト選択
  castSelector.addEventListener('change', (e) => {
    const castId = e.target.value;
    if (castId) {
      loadCastData(castId);
      uploadSection.classList.remove('hidden');
    } else {
      uploadSection.classList.add('hidden');
      resultsContainer.innerHTML = '';
      searchSection.classList.add('hidden');
      // キャストが選択解除されたら日付入力も無効化
      rangeStartDateInput.disabled = true;
      rangeEndDateInput.disabled = true;
      rangeSummaryButton.disabled = true;
    }
  });

  // キャスト追加ボタン
  addCastButton.addEventListener('click', handleAddCast);

  // キャスト追加Enterキー
  newCastNameInput.addEventListener('keypress', (e) => {
    if (e.key === 'Enter') {
      handleAddCast();
    }
  });

  // ファイル入力
  fileInput.addEventListener('change', handleFileSelect);

  // 検索入力
  searchInput.addEventListener('input', applySearchFilter);

  // 期間集計ボタン
  rangeSummaryButton.addEventListener('click', handleRangeSummary);

  // サマリー期間選択ボタン
  summaryAllButton.addEventListener('click', () => handleSummaryPeriodChange('all'));
  summaryMonthlyButton.addEventListener('click', () => handleSummaryPeriodChange('monthly'));
  summaryWeeklyButton.addEventListener('click', () => handleSummaryPeriodChange('weekly'));
}

/**
 * サマリー期間の変更を処理します。
 */
function handleSummaryPeriodChange(period) {
  currentSummaryPeriod = period;

  // ボタンのスタイルを更新
  [summaryAllButton, summaryMonthlyButton, summaryWeeklyButton].forEach(btn => {
    btn.classList.remove('bg-blue-600', 'hover:bg-blue-700');
    btn.classList.add('bg-gray-500', 'hover:bg-gray-600');
  });

  // 選択されたボタンのスタイルを更新
  const selectedButton = period === 'all' ? summaryAllButton :
    period === 'monthly' ? summaryMonthlyButton : summaryWeeklyButton;
  selectedButton.classList.remove('bg-gray-500', 'hover:bg-gray-600');
  selectedButton.classList.add('bg-blue-600', 'hover:bg-blue-700');

  // 現在のデータがある場合は再表示
  if (Object.keys(globalDailyStats).length > 0) {
    updateSummaryDisplay();
  }
}

/**
 * サマリー表示を更新します。
 */
function updateSummaryDisplay() {
  const filteredStats = filterStatsByPeriod(globalDailyStats, currentSummaryPeriod);

  // フィルタリングされたデータから統計を計算
  const productStats = {};
  let totalRevenue = 0;
  let totalQuantity = 0;

  for (const [dateStr, dayData] of Object.entries(filteredStats)) {
    totalRevenue += dayData.revenue || 0;
    totalQuantity += dayData.quantity || 0;

    // 商品別統計を集計
    for (const [productName, productData] of Object.entries(dayData.products || {})) {
      if (!productStats[productName]) {
        productStats[productName] = { quantity: 0, revenue: 0, uniqueUsers: new Set() };
      }
      productStats[productName].quantity += productData.quantity || 0;
      productStats[productName].revenue += productData.revenue || 0;
    }
  }

  // サマリーカードを更新
  const summaryCard = document.querySelector('.bg-white.rounded-xl.shadow-lg.p-6.border.border-gray-200');
  if (summaryCard) {
    summaryCard.innerHTML = createSummaryCardHTML(totalRevenue, totalQuantity, productStats, currentSummaryPeriod);
  }
}

/**
 * 期間に応じて統計をフィルタリングします。
 */
function filterStatsByPeriod(dailyStats, period) {
  const now = new Date();
  const filteredStats = {};

  for (const [dateStr, dayData] of Object.entries(dailyStats)) {
    const date = new Date(dateStr);
    let include = false;

    switch (period) {
      case 'monthly':
        // 過去30日
        const thirtyDaysAgo = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);
        include = date >= thirtyDaysAgo;
        break;
      case 'weekly':
        // 過去7日
        const sevenDaysAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
        include = date >= sevenDaysAgo;
        break;
      case 'all':
      default:
        include = true;
        break;
    }

    if (include) {
      filteredStats[dateStr] = dayData;
    }
  }

  return filteredStats;
}

/**
 * 会社グループ管理UIを有効化します。
 */
function enableCompanyGroupManagement() {
  companyGroupSelector.disabled = false;
  newCompanyGroupInput.disabled = false;
  addCompanyGroupButton.disabled = false;
}

/**
 * キャスト管理UIを有効化します。
 */
function enableCastManagement() {
  castSelector.disabled = false;
  newCastNameInput.disabled = false;
  addCastButton.disabled = false;
  castLoadingMessage.classList.add('hidden');

  // キャストが読み込めたら日付選択も有効化
  rangeStartDateInput.disabled = false;
  rangeEndDateInput.disabled = false;
  rangeSummaryButton.disabled = false;
}

/**
 * Firestoreから会社グループ一覧を読み込みます。
 */
function loadCompanyGroups() {
  console.log("会社グループ一覧の読み込み開始...");
  const companyGroupsColRef = collection(db, `artifacts/${appId}/public/data/companyGroups`);
  const q = query(companyGroupsColRef);

  onSnapshot(q, (snapshot) => {
    console.log("会社グループ一覧のデータ更新を検知");
    const companyGroups = [];
    snapshot.forEach((doc) => {
      companyGroups.push({ id: doc.id, name: doc.data().name });
    });

    // 名前でソート
    companyGroups.sort((a, b) => a.name.localeCompare(b.name, 'ja'));

    // ドロップダウンを更新
    const currentCompanyGroupId = companyGroupSelector.value;
    companyGroupSelector.innerHTML = '<option value="">選択してください</option>';
    companyGroups.forEach(group => {
      const option = document.createElement('option');
      option.value = group.id;
      option.textContent = group.name;
      companyGroupSelector.appendChild(option);
    });

    // 選択状態を復元
    if (currentCompanyGroupId) {
      companyGroupSelector.value = currentCompanyGroupId;
    }

    // UIを有効化
    enableCompanyGroupManagement();
    console.log("会社グループ一覧の読み込み完了。");

  }, (error) => {
    console.error("会社グループ一覧の読み込みに失敗:", error);
    companyGroupError.textContent = '会社グループ一覧の読み込みに失敗しました。';
  });
}

/**
 * 指定された会社グループのキャスト一覧を読み込みます。
 */
function loadCastsForCompanyGroup(companyGroupId) {
  console.log(`会社グループ ${companyGroupId} のキャスト一覧の読み込み開始...`);
  const castsColRef = collection(db, `artifacts/${appId}/public/data/companyGroups/${companyGroupId}/casts`);
  const q = query(castsColRef);

  onSnapshot(q, (snapshot) => {
    console.log("キャスト一覧のデータ更新を検知");
    const casts = [];
    snapshot.forEach((doc) => {
      casts.push({ id: doc.id, name: doc.data().name });
    });

    // 名前でソート
    casts.sort((a, b) => a.name.localeCompare(b.name, 'ja'));

    // ドロップダウンを更新
    const currentCastId = castSelector.value;
    castSelector.innerHTML = '<option value="">選択してください</option>';
    casts.forEach(cast => {
      const option = document.createElement('option');
      option.value = cast.id;
      option.textContent = cast.name;
      castSelector.appendChild(option);
    });

    // 選択状態を復元
    if (currentCastId) {
      castSelector.value = currentCastId;
    }

    // UIを有効化
    enableCastManagement();
    console.log("キャスト一覧の読み込み完了。");

  }, (error) => {
    console.error("キャスト一覧の読み込みに失敗:", error);
    castError.textContent = 'キャスト一覧の読み込みに失敗しました。';
    castLoadingMessage.textContent = 'エラー';
    castLoadingMessage.classList.add('text-red-500');
  });
}

/**
 * 新しい会社グループをFirestoreに追加します。
 */
async function handleAddCompanyGroup() {
  const companyGroupName = newCompanyGroupInput.value.trim();
  if (!companyGroupName) {
    companyGroupError.textContent = '会社名を入力してください。';
    return;
  }

  companyGroupError.textContent = '';
  addCompanyGroupButton.disabled = true;

  try {
    // ドキュメントIDを会社名から自動生成
    const hashBuffer = await crypto.subtle.digest('SHA-1', new TextEncoder().encode(companyGroupName));
    const hashArray = Array.from(new Uint8Array(hashBuffer));
    const companyGroupId = hashArray.map(b => b.toString(16).padStart(2, '0')).join('');

    const companyGroupDocRef = doc(db, `artifacts/${appId}/public/data/companyGroups/${companyGroupId}`);

    await setDoc(companyGroupDocRef, {
      name: companyGroupName
    });

    console.log("会社グループ追加成功:", companyGroupName);
    newCompanyGroupInput.value = '';

  } catch (error) {
    console.error("会社グループ追加エラー:", error);
    companyGroupError.textContent = '会社グループの追加に失敗しました。';
  } finally {
    addCompanyGroupButton.disabled = false;
  }
}

/**
 * 新しいキャストをFirestoreに追加します。
 */
async function handleAddCast() {
  const castName = newCastNameInput.value.trim();
  const companyGroupId = companyGroupSelector.value;

  if (!castName) {
    castError.textContent = 'キャスト名を入力してください。';
    return;
  }

  if (!companyGroupId) {
    castError.textContent = '会社グループを選択してください。';
    return;
  }

  castError.textContent = '';
  addCastButton.disabled = true;

  try {
    // ドキュメントIDをキャスト名から自動生成（衝突を避けるため）
    const hashBuffer = await crypto.subtle.digest('SHA-1', new TextEncoder().encode(castName));
    const hashArray = Array.from(new Uint8Array(hashBuffer));
    const castId = hashArray.map(b => b.toString(16).padStart(2, '0')).join('');

    const castDocRef = doc(db, `artifacts/${appId}/public/data/companyGroups/${companyGroupId}/casts/${castId}`);

    await setDoc(castDocRef, {
      name: castName
    });

    console.log("キャスト追加成功:", castName);
    newCastNameInput.value = '';

  } catch (error) {
    console.error("キャスト追加エラー:", error);
    castError.textContent = 'キャストの追加に失敗しました。';
  } finally {
    addCastButton.disabled = false;
  }
}

/**
 * ファイルが選択されたときに処理を開始します。
 * @param {Event} event - ファイル選択イベント
 */
function handleFileSelect(event) {
  const file = event.target.files[0];
  const castId = castSelector.value;
  if (!file || !castId) {
    return;
  }

  resultsContainer.innerHTML = '';
  loadingIndicator.classList.remove('hidden');
  searchSection.classList.add('hidden');

  const reader = new FileReader();

  reader.onload = async function (e) {
    try {
      const csvText = e.target.result;
      const { header, records } = parseCSV(csvText);

      // Firestoreへのバッチ書き込み
      await saveDataToFirestore(castId, records, header);

      // 完了後、自動的にデータを再読み込み・分析
      await loadCastData(castId);

      console.log("CSVデータの保存・分析が完了しました。");

    } catch (error) {
      console.error("エラーが発生しました:", error);
      displayError("ファイルの処理中にエラーが発生しました。" + error.message);
    } finally {
      loadingIndicator.classList.add('hidden');
      fileInput.value = ''; // ファイル選択をリセット
    }
  };

  reader.onerror = function () {
    displayError("ファイルの読み込みに失敗しました。");
    loadingIndicator.classList.add('hidden');
  };

  reader.readAsText(file, 'UTF-8');
}

/**
 * CSV文字列をパースしてヘッダーとデータ記録に分割します。
 * @param {string} csvText - CSVファイルの全テキスト
 * @returns {{header: string[], records: string[][]}} パースされたデータ
 */
function parseCSV(csvText) {
  const lines = csvText.replace(/\r/g, '').trim().split('\n');

  // Fantia CSVのヘッダーは3行目にある
  if (lines.length < 4) {
    throw new Error("CSVデータが不十分か、形式が正しくありません。");
  }

  const parseLine = (line) => {
    const result = [];
    let field = '';
    let inQuotes = false;

    for (let i = 0; i < line.length; i++) {
      const char = line[i];

      if (char === '"') {
        if (inQuotes && line[i + 1] === '"') {
          field += '"';
          i++;
        } else {
          inQuotes = !inQuotes;
        }
      } else if (char === ',' && !inQuotes) {
        result.push(field.trim());
        field = '';
      } else {
        field += char;
      }
    }
    result.push(field.trim());
    return result;
  };

  const headerLine = lines[2]; // ヘッダーは3行目
  const dataLines = lines.slice(3); // データは4行目以降

  const header = headerLine.split(',').map(h => h.trim());
  const records = dataLines.filter(line => line.trim() !== '').map(line => parseLine(line));

  // 必要な列のインデックスを確認
  const requiredColumns = ['注文ステータス', '注文日時', '商品名', '数量', 'ユーザーID', '合計金額（税込）', '注文ID'];
  const indices = {};
  requiredColumns.forEach(colName => {
    const index = header.indexOf(colName);
    if (index === -1) {
      throw new Error(`必要な列が見つかりません: ${colName}`);
    }
    indices[colName] = index;
  });

  console.log("CSVパース成功。ヘッダー:", header, "レコード数:", records.length);
  return { header, records, indices };
}

/**
 * パースされたCSVデータをFirestoreにバッチ書き込みします。
 * 注文IDをドキュメントIDとして使用し、データを上書き（set）します。
 */
async function saveDataToFirestore(castId, records, header) {
  console.log(`Firestoreへのバッチ書き込み開始... (${records.length}件)`);

  // 必要な列のインデックスを取得
  const indices = {
    status: header.indexOf('注文ステータス'),
    date: header.indexOf('注文日時'),
    productName: header.indexOf('商品名'),
    quantity: header.indexOf('数量'),
    userId: header.indexOf('ユーザーID'),
    price: header.indexOf('合計金額（税込）'),
    orderId: header.indexOf('注文ID')
  };

  if (indices.orderId === -1) {
    throw new Error("CSVに「注文ID」列が見つかりません。データの上書き・マージに必須です。");
  }
  if (Object.values(indices).includes(-1)) {
    throw new Error("必要な列（注文ステータス、注文日時など）が見つかりません。");
  }

  // バッチ処理の準備
  let batch = writeBatch(db);
  let operationCount = 0;
  const companyGroupId = companyGroupSelector.value;
  const collectionRef = collection(db, `artifacts/${appId}/public/data/companyGroups/${companyGroupId}/casts/${castId}/orders`);

  for (const record of records) {
    if (record.length < header.length) {
      console.warn('列数がヘッダーと一致しないため、この行をスキップします:', record);
      continue;
    }

    const orderId = record[indices.orderId];
    if (!orderId) {
      console.warn('注文IDが空のため、この行をスキップします:', record);
      continue;
    }

    // Firestoreに保存するデータオブジェクトを作成
    const orderData = {
      orderId: orderId,
      status: record[indices.status],
      orderDate: record[indices.date],
      productName: record[indices.productName],
      quantity: parseInt(record[indices.quantity], 10) || 0,
      userId: record[indices.userId],
      price: parseInt(record[indices.price], 10) || 0,
    };

    // 注文IDをドキュメントIDとして設定
    const docRef = doc(collectionRef, orderId);
    batch.set(docRef, orderData); // set = 常に上書き

    operationCount++;

    // Firestoreのバッチ書き込みは500件ごとにコミット
    if (operationCount >= 500) {
      await batch.commit();
      batch = writeBatch(db); // 新しいバッチを開始
      operationCount = 0;
      console.log("バッチをコミットしました (500件)");
    }
  }

  // 残りのバッチをコミット
  if (operationCount > 0) {
    await batch.commit();
    console.log(`最後のバッチをコミットしました (${operationCount}件)`);
  }

  console.log("Firestoreへのバッチ書き込み完了。");
}

/**
 * 指定されたキャストの注文データをFirestoreから読み込み、分析します。
 */
async function loadCastData(castId) {
  console.log(`キャストデータ(ID: ${castId})の読み込みと分析を開始...`);
  resultsContainer.innerHTML = '';
  loadingIndicator.classList.remove('hidden');
  searchSection.classList.add('hidden');

  try {
    const companyGroupId = companyGroupSelector.value;
    const ordersColRef = collection(db, `artifacts/${appId}/public/data/companyGroups/${companyGroupId}/casts/${castId}/orders`);
    const snapshot = await getDocs(ordersColRef);

    const records = [];
    snapshot.forEach(doc => {
      records.push(doc.data());
    });

    console.log(`データ読み込み完了。${records.length}件の注文データを分析します。`);

    if (records.length === 0) {
      displayError("このキャストにはまだ注文データがありません。CSVをアップロードしてください。");
      searchSection.classList.add('hidden');
      return;
    }

    // 読み込んだデータを分析
    const { dailyStats, productStats, totalRevenue, totalQuantity } = analyzeDataFromFirestore(records);

    // グローバル変数に日別データを保存
    globalDailyStats = dailyStats;

    // 結果を表示
    displayResults(dailyStats, productStats, totalRevenue, totalQuantity);

    searchSection.classList.remove('hidden');

  } catch (error) {
    console.error("キャストデータの読み込み・分析エラー:", error);
    displayError("データの分析中にエラーが発生しました: " + error.message);
  } finally {
    loadingIndicator.classList.add('hidden');
  }
}


/**
 * Firestoreから読み込んだデータを分析し、統計情報を計算します。
 * @param {object[]} records - Firestoreの注文ドキュメントの配列
 * @returns {object} 分析結果
 */
function analyzeDataFromFirestore(records) {
  const dailyStats = {};
  const productStats = {};
  let totalRevenue = 0;
  let totalQuantity = 0;

  for (const record of records) {
    // "取引完了" のデータのみを分析対象とする
    if (record.status === '取引完了') {
      const date = record.orderDate.split(' ')[0]; // YYYY-MM-DD 形式
      const productName = record.productName;
      const quantity = record.quantity || 0;
      const userId = record.userId;
      const price = record.price || 0;

      totalRevenue += price;
      totalQuantity += quantity;

      // --- 日別統計 ---
      if (!dailyStats[date]) {
        dailyStats[date] = { quantity: 0, revenue: 0, products: {} };
      }
      dailyStats[date].quantity += quantity;
      dailyStats[date].revenue += price;

      // --- 日別・商品別統計 ---
      if (!dailyStats[date].products[productName]) {
        dailyStats[date].products[productName] = { quantity: 0, revenue: 0 };
      }
      dailyStats[date].products[productName].quantity += quantity;
      dailyStats[date].products[productName].revenue += price;

      // --- 商品別統計 ---
      if (!productStats[productName]) {
        productStats[productName] = { quantity: 0, revenue: 0, uniqueUsers: new Set() };
      }
      productStats[productName].quantity += quantity;
      productStats[productName].revenue += price;
      productStats[productName].uniqueUsers.add(userId);
    }
  }
  console.log("データ分析完了。");
  return { dailyStats, productStats, totalRevenue, totalQuantity };
}

/**
 * 分析結果をHTMLとして画面に表示します。
 */
function displayResults(dailyStats, productStats, totalRevenue, totalQuantity) {
  resultsContainer.innerHTML = `
            ${createSummaryCard(totalRevenue, totalQuantity, productStats)}
            <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
                ${createProductStatsTable(productStats)}
                ${createDailyStatsTable(dailyStats)}
            </div>
        `;
  // 検索フィルターをリセット（新しいデータが表示されたため）
  applySearchFilter();
}

/**
 * 全体サマリーカードのHTMLを生成します。
 */
function createSummaryCard(totalRevenue, totalQuantity, productStats) {
  const uniqueProductCount = Object.keys(productStats).length;
  const periodLabel = currentSummaryPeriod === 'all' ? '全体' :
    currentSummaryPeriod === 'monthly' ? '月間' : '週間';

  return `
        <div class="bg-white rounded-xl shadow-lg p-6 border border-gray-200">
            <h2 class="text-xl font-bold text-gray-800 mb-4">${periodLabel}サマリー（取引完了のみ）</h2>
            <div class="grid grid-cols-1 sm:grid-cols-3 gap-4 text-center">
                <div>
                    <p class="text-sm text-gray-500">総売上金額</p>
                    <p class="text-2xl font-semibold text-blue-600">${totalRevenue.toLocaleString()}円</p>
                </div>
                <div>
                    <p class="text-sm text-gray-500">総販売個数</p>
                    <p class="text-2xl font-semibold text-blue-600">${totalQuantity.toLocaleString()}個</p>
                </div>
                <div>
                    <p class="text-sm text-gray-500">販売商品数</p>
                    <p class="text-2xl font-semibold text-blue-600">${uniqueProductCount}種類</p>
                </div>
            </div>
        </div>
        `;
}

/**
 * サマリーカードのHTMLを生成します（期間指定版）。
 */
function createSummaryCardHTML(totalRevenue, totalQuantity, productStats, period) {
  const uniqueProductCount = Object.keys(productStats).length;
  const periodLabel = period === 'all' ? '全体' :
    period === 'monthly' ? '月間' : '週間';

  return `
        <h2 class="text-xl font-bold text-gray-800 mb-4">${periodLabel}サマリー（取引完了のみ）</h2>
        <div class="grid grid-cols-1 sm:grid-cols-3 gap-4 text-center">
            <div>
                <p class="text-sm text-gray-500">総売上金額</p>
                <p class="text-2xl font-semibold text-blue-600">${totalRevenue.toLocaleString()}円</p>
            </div>
            <div>
                <p class="text-sm text-gray-500">総販売個数</p>
                <p class="text-2xl font-semibold text-blue-600">${totalQuantity.toLocaleString()}個</p>
            </div>
            <div>
                <p class="text-sm text-gray-500">販売商品数</p>
                <p class="text-2xl font-semibold text-blue-600">${uniqueProductCount}種類</p>
            </div>
        </div>
        `;
}

/**
 * 商品別レポートのHTMLを生成します。
 */
function createProductStatsTable(productStats) {
  const sortedProducts = Object.entries(productStats).sort(([, a], [, b]) => b.revenue - a.revenue);

  let tableRows = sortedProducts.map(([name, stats]) => {
    const avgPurchase = stats.uniqueUsers.size > 0 ? stats.quantity / stats.uniqueUsers.size : 0;
    // data-name属性に商品名を設定（検索用）
    return `
                <tr class="border-b border-gray-200 hover:bg-gray-50" data-search-name="${name.toLowerCase()}">
                    <td class="p-3 text-sm text-gray-700">${name}</td>
                    <td class="p-3 text-right font-medium">${stats.quantity.toLocaleString()}</td>
                    <td class="p-3 text-right">${stats.uniqueUsers.size.toLocaleString()}</td>
                    <td class="p-3 text-right">${avgPurchase.toFixed(2)}</td>
                    <td class="p-3 text-right font-semibold text-green-600">${stats.revenue.toLocaleString()}円</td>
                </tr>
            `;
  }).join('');

  if (!tableRows) {
    tableRows = '<tr><td colspan="5" class="text-center p-4 text-gray-500">データがありません。</td></tr>';
  }

  return `
            <div class="bg-white rounded-xl shadow-lg p-6 border border-gray-200">
                <h2 class="text-xl font-bold text-gray-800 mb-4">商品別レポート</h2>
                <div class="overflow-x-auto max-h-[60vh]">
                    <table id="productStatsTable" class="w-full min-w-[600px]">
                        <thead class="bg-gray-50 sticky top-0">
                            <tr>
                                <th class="p-3 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">商品名</th>
                                <th class="p-3 text-right text-xs font-semibold text-gray-600 uppercase tracking-wider">販売個数</th>
                                <th class="p-3 text-right text-xs font-semibold text-gray-600 uppercase tracking-wider">購入者数</th>
                                <th class="p-3 text-right text-xs font-semibold text-gray-600 uppercase tracking-wider">平均/人</th>
                                <th class="p-3 text-right text-xs font-semibold text-gray-600 uppercase tracking-wider">売上</th>
                            </tr>
                        </thead>
                        <tbody>${tableRows}</tbody>
                    </table>
                </div>
            </div>
        `;
}

/**
 * 日別レポートのHTMLを生成します。
 */
function createDailyStatsTable(dailyStats) {
  const sortedDates = Object.keys(dailyStats).sort((a, b) => new Date(b) - new Date(a));

  let tableRows = sortedDates.map(date => `
            <tr class="border-b border-gray-200 hover:bg-gray-50 cursor-pointer" onclick="window.showDailyDetailsModal('${date}')">
                <td class="p-3 text-sm text-gray-700">${date}</td>
                <td class="p-3 text-right font-medium">${dailyStats[date].quantity.toLocaleString()}</td>
                <td class="p-3 text-right font-semibold text-green-600">${dailyStats[date].revenue.toLocaleString()}円</td>
            </tr>
        `).join('');

  if (!tableRows) {
    tableRows = '<tr><td colspan="3" class="text-center p-4 text-gray-500">データがありません。</td></tr>';
  }

  return `
            <div class="bg-white rounded-xl shadow-lg p-6 border border-gray-200">
                <h2 class="text-xl font-bold text-gray-800 mb-4">日別レポート (クリックで詳細)</h2>
                <div class="overflow-x-auto max-h-[60vh]">
                    <table class="w-full">
                        <thead class="bg-gray-50 sticky top-0">
                            <tr>
                                <th class="p-3 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">日付</th>
                                <th class="p-3 text-right text-xs font-semibold text-gray-600 uppercase tracking-wider">販売個数</th>
                                <th class="p-3 text-right text-xs font-semibold text-gray-600 uppercase tracking-wider">売上</th>
                            </tr>
                        </thead>
                        <tbody>${tableRows}</tbody>
                    </table>
                </div>
            </div>
        `;
}

/**
 * エラーメッセージを表示します。
 */
function displayError(message) {
  resultsContainer.innerHTML = `
            <div class="bg-red-100 border-l-4 border-red-500 text-red-700 p-4 rounded-lg" role="alert">
                <p class="font-bold">エラー</p>
                <p>${message}</p>
            </div>
        `;
}

/**
 * 期間指定レポートの集計処理
 */
function handleRangeSummary() {
  const startDateStr = rangeStartDateInput.value;
  const endDateStr = rangeEndDateInput.value;

  if (!startDateStr || !endDateStr) {
    alert("開始日と終了日を両方選択してください。"); // このalertは許容範囲
    return;
  }

  const startDate = new Date(startDateStr);
  const endDate = new Date(endDateStr);

  if (startDate > endDate) {
    alert("終了日は開始日より後の日付を選択してください。");
    return;
  }

  // 終了日の時刻を 23:59:59 に設定して、その日全体が含まれるようにする
  endDate.setHours(23, 59, 59, 999);

  console.log(`期間集計: ${startDateStr} から ${endDateStr} まで`);

  // globalDailyStats から該当期間のデータを抽出して集計
  const rangeProductStats = {};
  let rangeTotalRevenue = 0;
  let rangeTotalQuantity = 0;
  let rangeUniqueUsers = new Set(); // 期間内のユニークユーザー

  for (const [dateStr, dailyData] of Object.entries(globalDailyStats)) {
    const currentDate = new Date(dateStr);
    if (currentDate >= startDate && currentDate <= endDate) {
      // 期間内の総売上・総個数を加算
      rangeTotalRevenue += dailyData.revenue;
      rangeTotalQuantity += dailyData.quantity;

      // 商品別データをマージ
      for (const [productName, productData] of Object.entries(dailyData.products)) {
        if (!rangeProductStats[productName]) {
          rangeProductStats[productName] = { quantity: 0, revenue: 0 };
        }
        rangeProductStats[productName].quantity += productData.quantity;
        rangeProductStats[productName].revenue += productData.revenue;
      }
    }
  }

  // 期間内の購入者数を計算 (元のデータに購入者情報がないため、この実装では商品別レポートから取得は難しい)
  // このデモでは、期間内の商品別集計と総合計を表示する

  // モーダルタイトル設定
  rangeModalTitle.textContent = `集計レポート (${startDateStr} 〜 ${endDateStr})`;

  // 期間内のユニークユーザー数を計算
  for (const [dateStr, dailyData] of Object.entries(globalDailyStats)) {
    const currentDate = new Date(dateStr);
    if (currentDate >= startDate && currentDate <= endDate) {
      // この日のデータからユニークユーザーを取得（実際のデータ構造に応じて調整が必要）
      // 現在の実装では、日別データにユーザー情報がないため、商品別統計から推定
    }
  }

  // 一人当たりの平均購入数を計算（ユニークユーザー数が0の場合は0を表示）
  const avgPurchasePerUser = rangeUniqueUsers.size > 0 ? (rangeTotalQuantity / rangeUniqueUsers.size).toFixed(2) : '0.00';

  // モーダル内容（サマリー）
  let modalHTML = `
             <div class="bg-blue-50 rounded-lg p-4 mb-6">
                 <h3 class="text-lg font-bold text-gray-800 mb-3">期間サマリー</h3>
                 <div class="grid grid-cols-1 sm:grid-cols-3 gap-4 text-center">
                     <div>
                         <p class="text-sm text-gray-500">期間売上金額</p>
                         <p class="text-2xl font-semibold text-blue-600">${rangeTotalRevenue.toLocaleString()}円</p>
                     </div>
                     <div>
                         <p class="text-sm text-gray-500">期間販売個数</p>
                         <p class="text-2xl font-semibold text-blue-600">${rangeTotalQuantity.toLocaleString()}個</p>
                     </div>
                     <div>
                         <p class="text-sm text-gray-500">一人当たり平均購入数</p>
                         <p class="text-2xl font-semibold text-blue-600">${avgPurchasePerUser}個</p>
                     </div>
                 </div>
             </div>
            `;

  // モーダル内容（商品別テーブル）
  const sortedProducts = Object.entries(rangeProductStats).sort(([, a], [, b]) => b.revenue - a.revenue);

  const productRows = sortedProducts.map(([name, stats]) => `
             <tr class="border-t border-gray-200" data-search-name="${name.toLowerCase()}">
                 <td class="p-3 text-sm text-gray-600">${name}</td>
                 <td class="p-3 text-right font-medium">${stats.quantity.toLocaleString()}</td>
                 <td class="p-3 text-right text-green-600">${stats.revenue.toLocaleString()}円</td>
             </tr>
         `).join('');

  modalHTML += `
             <h3 class="text-lg font-bold text-gray-800 mb-3">期間中の商品別売上</h3>
             <table id="modalRangeProductStatsTable" class="w-full text-sm">
                 <thead class="border-b">
                     <tr>
                         <th class="pb-2 text-left font-semibold text-gray-600">商品名</th>
                         <th class="pb-2 text-right font-semibold text-gray-600">販売個数</th>
                         <th class="pb-2 text-right font-semibold text-gray-600">売上</th>
                     </tr>
                 </thead>
                 <tbody>
                     ${productRows.length > 0 ? productRows : '<tr><td colspan="3" class="text-center p-4 text-gray-500">該当期間にデータはありません。</td></tr>'}
                 </tbody>
             </table>
        `;

  rangeModalBody.innerHTML = modalHTML;
  showRangeSummaryModal();
}

/**
 * 検索フィルターを適用します。
 */
function applySearchFilter() {
  // searchInputが初期化される前に呼ばれる可能性があるため、存在チェック
  if (!searchInput) return;

  const query = searchInput.value.toLowerCase().trim();

  // 商品別レポートのフィルタリング
  const productTable = document.getElementById('productStatsTable');
  if (productTable) {
    const rows = productTable.getElementsByTagName('tbody')[0].getElementsByTagName('tr');
    for (const row of rows) {
      const name = row.dataset.searchName;
      if (name) {
        if (name.includes(query)) {
          row.style.display = '';
        } else {
          row.style.display = 'none';
        }
      }
    }
  }

  // (もしモーダルが開いていれば) モーダル内のフィルタリング
  const modalTable = document.getElementById('modalProductStatsTable');
  if (modalTable && !dailyDetailsModal.classList.contains('opacity-0')) {
    const modalRows = modalTable.getElementsByTagName('tbody')[0].getElementsByTagName('tr');
    for (const row of modalRows) {
      const name = row.dataset.searchName;
      if (name) {
        if (name.includes(query)) {
          row.style.display = '';
        } else {
          row.style.display = 'none';
        }
      }
    }
  }

  // (もし期間集計モーダルが開いていれば) モーダル内のフィルタリング
  const rangeModalTable = document.getElementById('modalRangeProductStatsTable');
  if (rangeModalTable && !rangeSummaryModal.classList.contains('opacity-0')) {
    const modalRows = rangeModalTable.getElementsByTagName('tbody')[0].getElementsByTagName('tr');
    for (const row of modalRows) {
      const name = row.dataset.searchName;
      if (name) {
        if (name.includes(query)) {
          row.style.display = '';
        } else {
          row.style.display = 'none';
        }
      }
    }
  }
}

// --- モーダル制御 ---
// グローバルスコープに関数を公開

/**
 * 日別詳細モーダルを表示します。
 * @param {string} date - 表示する日付 (YYYY-MM-DD)
 */
window.showDailyDetailsModal = (date) => {
  const data = globalDailyStats[date];
  if (!data) return;

  modalTitle.textContent = `${date} の商品別レポート`;

  const products = data.products;
  const sortedProducts = Object.entries(products).sort(([, a], [, b]) => b.quantity - a.quantity);

  const productRows = sortedProducts.map(([name, stats]) => `
            <tr class="border-t border-gray-200" data-search-name="${name.toLowerCase()}">
                <td class="p-3 text-sm text-gray-600">${name}</td>
                <td class="p-3 text-right font-medium">${stats.quantity.toLocaleString()}</td>
                <td class="p-3 text-right text-green-600">${stats.revenue.toLocaleString()}円</td>
            </tr>
        `).join('');

  modalBody.innerHTML = `
            <table id="modalProductStatsTable" class="w-full text-sm">
                <thead class="border-b">
                    <tr>
                        <th class="pb-2 text-left font-semibold text-gray-600">商品名</th>
                        <th class="pb-2 text-right font-semibold text-gray-600">販売個S数</th>
                        <th class="pb-2 text-right font-semibold text-gray-600">売上</th>
                    </tr>
                </thead>
                <tbody>
                    ${productRows}
                </tbody>
            </table>
        `;

  // モーダルを表示
  dailyDetailsModal.classList.remove('opacity-0', 'pointer-events-none');
  dailyDetailsModal.firstElementChild.classList.remove('scale-95');

  // 現在の検索フィルターをモーダルにも適用
  applySearchFilter();
}

/**
 * 日別詳細モーダルを非表示にします。
 */
window.hideDailyDetailsModal = () => {
  dailyDetailsModal.classList.add('opacity-0', 'pointer-events-none');
  dailyDetailsModal.firstElementChild.classList.add('scale-95');
}

/**
 * 期間集計モーダルを表示します。
 */
window.showRangeSummaryModal = () => {
  rangeSummaryModal.classList.remove('opacity-0', 'pointer-events-none');
  rangeSummaryModal.firstElementChild.classList.remove('scale-95');
  // 現在の検索フィルターをモーダルにも適用
  applySearchFilter();
}

/**
 * 期間集計モーダルを非表示にします。
 */
window.hideRangeSummaryModal = () => {
  rangeSummaryModal.classList.add('opacity-0', 'pointer-events-none');
  rangeSummaryModal.firstElementChild.classList.add('scale-95');
}

document.addEventListener('DOMContentLoaded', initializeMainApp);
