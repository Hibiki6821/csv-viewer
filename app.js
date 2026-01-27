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
  getDocs // setLogLevel は削除したよん！
} from "https://www.gstatic.com/firebasejs/11.6.1/firebase-firestore.js";

// Chart.js は index.html で読み込んでいるため、import は不要
// グローバル変数 Chart をそのまま使用します

// --- グローバル変数・定数 ---
let db, auth, userId, appId, correctPasswordHash;
let globalAllRecords = []; // フィルタリング前の全レコード
let globalDailyStats = {}; // 現在表示中の(フィルタ済み)日別データ
let globalDailyMetadata = {}; // 日別のアクティブユーザー数などのメタデータ
let globalProductMetadata = {}; // 商品別のアクティブユーザー数などのメタデータ
let currentSummaryPeriod = 'all'; // 現在のサマリー期間 ('all', 'monthly', 'weekly')
let currentProductTypeFilter = 'all'; // 現在選択中の商品タイプ ('all', 'ダウンロード商品', 'くじ', etc.)
let availableProductTypes = new Set(); // データに含まれる商品タイプのセット
let includeFreeProductsInAnalysis = false; // ユーザー分析に無料商品を含めるかどうか

// Chart.jsのインスタンス保持用
let salesChartInstance = null;
let genreChartInstance = null;

// --- DOM要素 (認証画面) ---
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
let companyGroupSelector, newCompanyGroupInput, addCompanyGroupButton, companyGroupError,
  castSelector, newCastNameInput, addCastButton,
  castError, castLoadingMessage, uploadSection, fileInput, searchSection,
  searchInput, loadingIndicator, resultsContainer,
  dailyDetailsModal, modalTitle, modalBody,
  rangeStartDateInput, rangeEndDateInput, rangeSummaryButton,
  rangeSummaryModal, rangeModalTitle, rangeModalBody,
  summaryAllButton, summaryMonthlyButton, summaryWeeklyButton,
  productTypeFilterContainer, productTypeTabs;

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
  productTypeFilterContainer = document.getElementById('productTypeFilterContainer');
  productTypeTabs = document.getElementById('productTypeTabs');

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
  // 1. Firebase設定
  try {
    const firebaseConfig = {
      apiKey: "AIzaSyDDz9cs9Wgx8Npjrh7FwUB4kF1h8Zwsiik",
      authDomain: "fantia-csv.firebaseapp.com",
      projectId: "fantia-csv",
      storageBucket: "fantia-csv.firebasestorage.app",
      messagingSenderId: "457081920405",
      appId: "1:457081920405:web:6a33cb7f82e1ff7739f49c"
    };
    appId = 'fantia-analyzer-app';

    // 2. Firebase初期化
    const app = initializeApp(firebaseConfig);
    auth = getAuth(app);
    db = getFirestore(app);
    // setLogLevel('Debug'); // ← デバッグログが出ちゃうので削除したよ！

    // 3. 認証状態の監視
    onAuthStateChanged(auth, async (user) => {
      if (user) {
        userId = user.uid;
        console.log("Firebase 認証成功.");

        try {
          const passDocRef = doc(db, `artifacts/${appId}/public/data/config/password`);
          const passDocSnap = await getDoc(passDocRef);

          if (passDocSnap.exists()) {
            correctPasswordHash = passDocSnap.data().hash;
            // console.log("パスワードの取得成功。"); // 念のためログ出力を削除

            if (getCookie('auth_token_lottery_analyzer') === correctPasswordHash) {
              showMainContentAndInitApp();
              loadCompanyGroups();
            } else {
              enablePasswordForm();
            }
          } else {
            console.error("Firestoreにパスワードが設定されていません。");
            passwordLoadingMessage.textContent = 'エラー: 管理者が未設定です。';
            passwordLoadingMessage.classList.add('text-red-500');
          }
        } catch (err) {
          console.error("パスワードの取得に失敗:", err);
          passwordLoadingMessage.textContent = 'エラー: DB接続に失敗しました。';
          passwordLoadingMessage.classList.add('text-red-500');
        }

        setupPasswordFormListeners();

      } else {
        console.log("未認証状態。匿名サインインを実行します...");
        try {
          await signInAnonymously(auth);
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
  passwordForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    loginButton.disabled = true;
    loginButtonText.classList.add('hidden');
    loginButtonSpinner.classList.remove('hidden');

    const inputPassword = passwordInput.value;

    if (inputPassword === correctPasswordHash) {
      setCookie('auth_token_lottery_analyzer', inputPassword, 10);
      showMainContentAndInitApp();
      loadCompanyGroups();
    } else {
      errorMessage.classList.remove('hidden');
      passwordInput.value = '';
      loginButton.disabled = false;
      loginButtonText.classList.remove('hidden');
      loginButtonSpinner.classList.add('hidden');
    }
  });

  passwordInput.addEventListener('input', () => {
    errorMessage.classList.add('hidden');
  });

  if (showPasswordToggle) {
    showPasswordToggle.addEventListener('change', () => {
      passwordInput.type = showPasswordToggle.checked ? 'text' : 'password';
    });
  }
}

/**
 * メインアプリのイベントリスナーを設定します。
 */
function setupEventListeners() {
  // 会社グループ選択
  companyGroupSelector.addEventListener('change', (e) => {
    const companyGroupId = e.target.value;
    if (companyGroupId) {
      loadCastsForCompanyGroup(companyGroupId);
      summaryAllButton.disabled = false;
      summaryMonthlyButton.disabled = false;
      summaryWeeklyButton.disabled = false;
    } else {
      castSelector.innerHTML = '<option value="">会社グループを選択してください</option>';
      castSelector.disabled = true;
      uploadSection.classList.add('hidden');
      resultsContainer.innerHTML = '';
      searchSection.classList.add('hidden');
      productTypeFilterContainer.classList.add('hidden');
      summaryAllButton.disabled = true;
      summaryMonthlyButton.disabled = true;
      summaryWeeklyButton.disabled = true;
    }
  });

  addCompanyGroupButton.addEventListener('click', handleAddCompanyGroup);
  newCompanyGroupInput.addEventListener('keypress', (e) => {
    if (e.key === 'Enter') handleAddCompanyGroup();
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
      productTypeFilterContainer.classList.add('hidden');
      rangeStartDateInput.disabled = true;
      rangeEndDateInput.disabled = true;
      rangeSummaryButton.disabled = true;
    }
  });

  addCastButton.addEventListener('click', handleAddCast);
  newCastNameInput.addEventListener('keypress', (e) => {
    if (e.key === 'Enter') handleAddCast();
  });

  fileInput.addEventListener('change', handleFileSelect);
  searchInput.addEventListener('input', applySearchFilter);
  rangeSummaryButton.addEventListener('click', handleRangeSummary);

  summaryAllButton.addEventListener('click', () => handleSummaryPeriodChange('all'));
  summaryMonthlyButton.addEventListener('click', () => handleSummaryPeriodChange('monthly'));
  summaryWeeklyButton.addEventListener('click', () => handleSummaryPeriodChange('weekly'));
}

/**
 * サマリー期間の変更を処理します。
 */
function handleSummaryPeriodChange(period) {
  currentSummaryPeriod = period;

  [summaryAllButton, summaryMonthlyButton, summaryWeeklyButton].forEach(btn => {
    btn.classList.remove('bg-blue-600', 'hover:bg-blue-700');
    btn.classList.add('bg-gray-500', 'hover:bg-gray-600');
  });

  const selectedButton = period === 'all' ? summaryAllButton :
    period === 'monthly' ? summaryMonthlyButton : summaryWeeklyButton;
  selectedButton.classList.remove('bg-gray-500', 'hover:bg-gray-600');
  selectedButton.classList.add('bg-blue-600', 'hover:bg-blue-700');

  // ビューの再描画 (グローバルデータを使用)
  updateViewFromGlobalData();
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

  rangeStartDateInput.disabled = false;
  rangeEndDateInput.disabled = false;
  rangeSummaryButton.disabled = false;
}

/**
 * Firestoreから会社グループ一覧を読み込みます。
 */
function loadCompanyGroups() {
  const companyGroupsColRef = collection(db, `artifacts/${appId}/public/data/companyGroups`);
  const q = query(companyGroupsColRef);

  onSnapshot(q, (snapshot) => {
    const companyGroups = [];
    snapshot.forEach((doc) => {
      companyGroups.push({ id: doc.id, name: doc.data().name });
    });
    companyGroups.sort((a, b) => a.name.localeCompare(b.name, 'ja'));

    const currentCompanyGroupId = companyGroupSelector.value;
    companyGroupSelector.innerHTML = '<option value="">選択してください</option>';
    companyGroups.forEach(group => {
      const option = document.createElement('option');
      option.value = group.id;
      option.textContent = group.name;
      companyGroupSelector.appendChild(option);
    });

    if (currentCompanyGroupId) {
      companyGroupSelector.value = currentCompanyGroupId;
    }
    enableCompanyGroupManagement();

  }, (error) => {
    console.error("会社グループ一覧の読み込みに失敗:", error);
    companyGroupError.textContent = '会社グループ一覧の読み込みに失敗しました。';
  });
}

/**
 * 指定された会社グループのキャスト一覧を読み込みます。
 */
function loadCastsForCompanyGroup(companyGroupId) {
  const castsColRef = collection(db, `artifacts/${appId}/public/data/companyGroups/${companyGroupId}/casts`);
  const q = query(castsColRef);

  onSnapshot(q, (snapshot) => {
    const casts = [];
    snapshot.forEach((doc) => {
      casts.push({ id: doc.id, name: doc.data().name });
    });
    casts.sort((a, b) => a.name.localeCompare(b.name, 'ja'));

    const currentCastId = castSelector.value;
    castSelector.innerHTML = '<option value="">選択してください</option>';
    casts.forEach(cast => {
      const option = document.createElement('option');
      option.value = cast.id;
      option.textContent = cast.name;
      castSelector.appendChild(option);
    });

    if (currentCastId) {
      castSelector.value = currentCastId;
    }
    enableCastManagement();

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
    const hashBuffer = await crypto.subtle.digest('SHA-1', new TextEncoder().encode(companyGroupName));
    const hashArray = Array.from(new Uint8Array(hashBuffer));
    const companyGroupId = hashArray.map(b => b.toString(16).padStart(2, '0')).join('');

    const companyGroupDocRef = doc(db, `artifacts/${appId}/public/data/companyGroups/${companyGroupId}`);
    await setDoc(companyGroupDocRef, { name: companyGroupName });

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
    const hashBuffer = await crypto.subtle.digest('SHA-1', new TextEncoder().encode(castName));
    const hashArray = Array.from(new Uint8Array(hashBuffer));
    const castId = hashArray.map(b => b.toString(16).padStart(2, '0')).join('');

    const castDocRef = doc(db, `artifacts/${appId}/public/data/companyGroups/${companyGroupId}/casts/${castId}`);
    await setDoc(castDocRef, { name: castName });

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
 */
function handleFileSelect(event) {
  const file = event.target.files[0];
  const castId = castSelector.value;
  if (!file || !castId) return;

  resultsContainer.innerHTML = '';
  loadingIndicator.classList.remove('hidden');
  searchSection.classList.add('hidden');
  productTypeFilterContainer.classList.add('hidden');

  const reader = new FileReader();

  reader.onload = async function (e) {
    try {
      const csvText = e.target.result;
      const { header, records } = parseCSV(csvText);
      await saveDataToFirestore(castId, records, header);
      await loadCastData(castId); // 完了後に再読み込み
      console.log("CSVデータの保存・分析が完了しました。");
    } catch (error) {
      console.error("エラーが発生しました:", error);
      displayError("ファイルの処理中にエラーが発生しました。" + error.message);
    } finally {
      loadingIndicator.classList.add('hidden');
      fileInput.value = '';
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
 * 商品のタイプ列にも対応。
 */
function parseCSV(csvText) {
  const lines = csvText.replace(/\r/g, '').trim().split('\n');

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

  const requiredColumns = ['注文ステータス', '注文日時', '商品名', '数量', 'ユーザーID', '合計金額（税込）', '注文ID'];
  requiredColumns.forEach(colName => {
    if (header.indexOf(colName) === -1) {
      throw new Error(`必要な列が見つかりません: ${colName}`);
    }
  });

  return { header, records };
}

/**
 * パースされたCSVデータをFirestoreにバッチ書き込みします。
 * 商品タイプ(productType)も保存します。
 */
async function saveDataToFirestore(castId, records, header) {
  console.log(`Firestoreへのバッチ書き込み開始... (${records.length}件)`);

  const indices = {
    status: header.indexOf('注文ステータス'),
    date: header.indexOf('注文日時'),
    productName: header.indexOf('商品名'),
    quantity: header.indexOf('数量'),
    userId: header.indexOf('ユーザーID'),
    price: header.indexOf('合計金額（税込）'),
    orderId: header.indexOf('注文ID'),
    productType: header.indexOf('商品のタイプ') // 新規追加（存在しない場合は-1）
  };

  let batch = writeBatch(db);
  let operationCount = 0;
  const companyGroupId = companyGroupSelector.value;
  const collectionRef = collection(db, `artifacts/${appId}/public/data/companyGroups/${companyGroupId}/casts/${castId}/orders`);

  for (const record of records) {
    if (record.length < header.length) continue;

    const orderId = record[indices.orderId];
    if (!orderId) continue;

    // 商品タイプを取得（列がない場合は '未分類' とする）
    let productType = '未分類';
    if (indices.productType !== -1 && record[indices.productType]) {
      productType = record[indices.productType];
    }

    const orderData = {
      orderId: orderId,
      status: record[indices.status],
      orderDate: record[indices.date],
      productName: record[indices.productName],
      quantity: parseInt(record[indices.quantity], 10) || 0,
      userId: record[indices.userId],
      price: parseInt(record[indices.price], 10) || 0,
      productType: productType
    };

    const docRef = doc(collectionRef, orderId);
    batch.set(docRef, orderData);

    operationCount++;
    if (operationCount >= 500) {
      await batch.commit();
      batch = writeBatch(db);
      operationCount = 0;
      console.log("バッチをコミットしました (500件)");
    }
  }

  if (operationCount > 0) {
    await batch.commit();
    console.log(`最後のバッチをコミットしました (${operationCount}件)`);
  }
}

/**
 * 指定されたキャストの注文データとメタデータをFirestoreから読み込み、分析します。
 */
async function loadCastData(castId) {
  console.log(`キャストデータ(ID: ${castId})の読み込みと分析を開始...`);
  resultsContainer.innerHTML = '';
  loadingIndicator.classList.remove('hidden');
  searchSection.classList.add('hidden');
  productTypeFilterContainer.classList.add('hidden');

  try {
    const companyGroupId = companyGroupSelector.value;
    
    // 注文データの読み込み
    const ordersColRef = collection(db, `artifacts/${appId}/public/data/companyGroups/${companyGroupId}/casts/${castId}/orders`);
    const snapshot = await getDocs(ordersColRef);

    globalAllRecords = [];
    snapshot.forEach(doc => {
      globalAllRecords.push(doc.data());
    });
    
    // 日別メタデータ（アクティブユーザー数など）の読み込み
    globalDailyMetadata = {};
    try {
      const metadataColRef = collection(db, `artifacts/${appId}/public/data/companyGroups/${companyGroupId}/casts/${castId}/dailyMetadata`);
      const metadataSnapshot = await getDocs(metadataColRef);
      metadataSnapshot.forEach(doc => {
        globalDailyMetadata[doc.id] = doc.data();
      });
      console.log("日別メタデータ読み込み完了:", Object.keys(globalDailyMetadata).length, "件");
    } catch (e) {
      console.warn("日別メタデータの読み込みに失敗 (初回はデータがない可能性があります):", e);
    }

    // 商品別メタデータ（アクティブユーザー数など）の読み込み
    globalProductMetadata = {};
    try {
      const productMetadataColRef = collection(db, `artifacts/${appId}/public/data/companyGroups/${companyGroupId}/casts/${castId}/productMetadata`);
      const productMetadataSnapshot = await getDocs(productMetadataColRef);
      productMetadataSnapshot.forEach(doc => {
        // ドキュメントIDはハッシュ化されている可能性があるため、フィールドのproductNameをキーにするのが確実だが、
        // 保存時にproductNameフィールドを含めている前提で処理する
        const data = doc.data();
        if (data.productName) {
            globalProductMetadata[data.productName] = data;
        }
      });
      console.log("商品別メタデータ読み込み完了:", Object.keys(globalProductMetadata).length, "件");
    } catch (e) {
      console.warn("商品別メタデータの読み込みに失敗 (初回はデータがない可能性があります):", e);
    }

    console.log(`データ読み込み完了。${globalAllRecords.length}件の注文データを取得。`);

    if (globalAllRecords.length === 0) {
      displayError("このキャストにはまだ注文データがありません。CSVをアップロードしてください。");
      searchSection.classList.add('hidden');
      return;
    }

    // 商品タイプの一覧を作成
    extractAvailableProductTypes(globalAllRecords);

    // 商品タイプ選択タブを描画
    renderProductTypeTabs();

    // デフォルトで「すべて」を選択して表示
    currentProductTypeFilter = 'all';
    updateViewFromGlobalData();

    searchSection.classList.remove('hidden');
    productTypeFilterContainer.classList.remove('hidden');

  } catch (error) {
    console.error("キャストデータの読み込み・分析エラー:", error);
    displayError("データの分析中にエラーが発生しました: " + error.message);
  } finally {
    loadingIndicator.classList.add('hidden');
  }
}

/**
 * 全レコードから存在する商品タイプを抽出します。
 */
function extractAvailableProductTypes(records) {
  availableProductTypes = new Set();
  records.forEach(record => {
    if (record.productType) {
      availableProductTypes.add(record.productType);
    } else {
      // 古いデータなどでフィールドがない場合
      availableProductTypes.add('未分類');
    }
  });
}

/**
 * 商品タイプ切り替えタブを描画します。
 */
function renderProductTypeTabs() {
  productTypeTabs.innerHTML = '';

  // 「すべて」タブ
  const allTab = document.createElement('button');
  allTab.textContent = 'すべて';
  allTab.dataset.type = 'all';
  allTab.className = 'px-4 py-2 rounded-lg font-medium text-sm transition-colors ' +
    (currentProductTypeFilter === 'all' ? 'bg-blue-600 text-white shadow-md' : 'bg-white text-gray-700 hover:bg-gray-100 border border-gray-300');
  allTab.onclick = () => handleProductTypeChange('all');
  productTypeTabs.appendChild(allTab);

  // 各商品タイプのタブ
  // 名前順にソートして表示
  const sortedTypes = Array.from(availableProductTypes).sort();
  sortedTypes.forEach(type => {
    const tab = document.createElement('button');
    tab.textContent = type;
    tab.dataset.type = type;
    tab.className = 'px-4 py-2 rounded-lg font-medium text-sm transition-colors ' +
      (currentProductTypeFilter === type ? 'bg-blue-600 text-white shadow-md' : 'bg-white text-gray-700 hover:bg-gray-100 border border-gray-300');
    tab.onclick = () => handleProductTypeChange(type);
    productTypeTabs.appendChild(tab);
  });
}

/**
 * 商品タイプが変更されたときの処理
 */
function handleProductTypeChange(type) {
  currentProductTypeFilter = type;
  
  // タブのスタイル更新
  const tabs = productTypeTabs.querySelectorAll('button');
  tabs.forEach(tab => {
    if (tab.dataset.type === type) {
      tab.className = 'px-4 py-2 rounded-lg font-medium text-sm transition-colors bg-blue-600 text-white shadow-md';
    } else {
      tab.className = 'px-4 py-2 rounded-lg font-medium text-sm transition-colors bg-white text-gray-700 hover:bg-gray-100 border border-gray-300';
    }
  });

  // データをフィルタリングして再表示
  updateViewFromGlobalData();
}

/**
 * グローバルデータから、現在のフィルタ設定（期間・商品タイプ）に基づいて表示を更新します。
 */
function updateViewFromGlobalData() {
  if (!globalAllRecords || globalAllRecords.length === 0) return;

  // 1. 商品タイプでフィルタリング
  let filteredRecords = globalAllRecords;
  if (currentProductTypeFilter !== 'all') {
    filteredRecords = globalAllRecords.filter(r => {
      const type = r.productType || '未分類';
      return type === currentProductTypeFilter;
    });
  }

  // 2. 期間フィルタリング (currentSummaryPeriodを使用)
  // 分析実行
  const { dailyStats, productStats, totalRevenue, totalQuantity } = analyzeFilteredData(filteredRecords, currentSummaryPeriod);

  // グローバルな日別統計を更新 (モーダル表示などで使用)
  globalDailyStats = dailyStats;

  // 結果表示
  displayResults(dailyStats, productStats, totalRevenue, totalQuantity);
}


/**
 * フィルタ済みのレコードデータを分析し、統計情報を計算します。
 */
function analyzeFilteredData(records, period) {
  const dailyStats = {};
  const productStats = {};
  let totalRevenue = 0;
  let totalQuantity = 0;

  const now = new Date();
  let startDate;

  // 期間フィルタの開始日設定
  switch (period) {
    case 'monthly':
      startDate = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);
      break;
    case 'weekly':
      startDate = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
      break;
    case 'all':
    default:
      startDate = new Date('2000-01-01'); // 全期間
      break;
  }

  for (const record of records) {
    if (record.status === '取引完了') {
      const orderDateStr = record.orderDate.split(' ')[0];
      const orderDate = new Date(orderDateStr);

      // 期間チェック
      if (orderDate >= startDate) {
        const productName = record.productName;
        const quantity = record.quantity || 0;
        const userId = record.userId;
        const price = record.price || 0;

        totalRevenue += price;
        totalQuantity += quantity;

        // --- 日別統計 ---
        if (!dailyStats[orderDateStr]) {
          dailyStats[orderDateStr] = { quantity: 0, revenue: 0, products: {}, uniqueUsers: new Set() };
        }
        dailyStats[orderDateStr].quantity += quantity;
        dailyStats[orderDateStr].revenue += price;
        dailyStats[orderDateStr].uniqueUsers.add(userId); // その日のユニークユーザー

        // --- 日別・商品別統計 ---
        if (!dailyStats[orderDateStr].products[productName]) {
          dailyStats[orderDateStr].products[productName] = { quantity: 0, revenue: 0 };
        }
        dailyStats[orderDateStr].products[productName].quantity += quantity;
        dailyStats[orderDateStr].products[productName].revenue += price;

        // --- 商品別統計 ---
        if (!productStats[productName]) {
          productStats[productName] = { quantity: 0, revenue: 0, uniqueUsers: new Set() };
        }
        productStats[productName].quantity += quantity;
        productStats[productName].revenue += price;
        productStats[productName].uniqueUsers.add(userId);
      }
    }
  }

  return { dailyStats, productStats, totalRevenue, totalQuantity };
}

/**
 * 分析結果をHTMLとして画面に表示します。
 */
function displayResults(dailyStats, productStats, totalRevenue, totalQuantity) {
  // ジャンル表示用のラベル作成
  let genreLabel = currentProductTypeFilter === 'all' ? '' : `（${currentProductTypeFilter}）`;

  resultsContainer.innerHTML = `
            ${createSummaryCard(totalRevenue, totalQuantity, productStats, genreLabel)}
            
            <!-- グラフエリア (2カラム) -->
            <div class="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-6">
                <div class="bg-white rounded-xl shadow-lg p-6 border border-gray-200 lg:col-span-2">
                    <h2 class="text-lg font-bold text-gray-800 mb-4">売上推移とCVR</h2>
                    <div class="relative h-64 w-full">
                        <canvas id="salesChart"></canvas>
                    </div>
                </div>
                <div class="bg-white rounded-xl shadow-lg p-6 border border-gray-200">
                    <h2 class="text-lg font-bold text-gray-800 mb-4">ジャンル別売上比率</h2>
                    <div class="relative h-64 w-full flex justify-center">
                        <canvas id="genreChart"></canvas>
                    </div>
                </div>
            </div>

            <!-- ユーザー分析エリア -->
            <div id="userAnalysisArea"></div>

            <div class="space-y-6"> <!-- 縦並びに変更: gridを削除しspace-yを使用 -->
                ${createProductStatsTable(productStats)}
                ${createDailyStatsTable(dailyStats)}
            </div>
        `;
  
  // グラフ描画
  renderCharts(dailyStats);
  
  // ユーザー分析描画
  renderUserAnalysis();

  applySearchFilter();
}

/**
 * グラフを描画します。
 */
function renderCharts(dailyStats) {
    // 1. 売上・CVR推移グラフ
    const ctxSales = document.getElementById('salesChart').getContext('2d');
    
    // 既存のチャートがあれば破棄
    if (salesChartInstance) {
        salesChartInstance.destroy();
    }

    const sortedDates = Object.keys(dailyStats).sort();
    const labels = sortedDates;
    const revenueData = sortedDates.map(date => dailyStats[date].revenue);
    const cvrData = sortedDates.map(date => {
        const activeUsers = globalDailyMetadata[date]?.activeUsers || 0;
        const purchaseUU = dailyStats[date].uniqueUsers.size;
        return activeUsers > 0 ? (purchaseUU / activeUsers) * 100 : 0;
    });

    salesChartInstance = new Chart(ctxSales, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [
                {
                    label: '売上 (円)',
                    data: revenueData,
                    backgroundColor: 'rgba(59, 130, 246, 0.5)', // blue-500
                    borderColor: 'rgb(59, 130, 246)',
                    borderWidth: 1,
                    yAxisID: 'y',
                },
                {
                    label: 'CVR (%)',
                    data: cvrData,
                    type: 'line',
                    borderColor: 'rgb(16, 185, 129)', // green-500
                    backgroundColor: 'rgba(16, 185, 129, 0.1)',
                    borderWidth: 2,
                    yAxisID: 'y1',
                    tension: 0.1
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    type: 'linear',
                    display: true,
                    position: 'left',
                    title: { display: true, text: '売上金額' }
                },
                y1: {
                    type: 'linear',
                    display: true,
                    position: 'right',
                    title: { display: true, text: 'CVR (%)' },
                    grid: { drawOnChartArea: false }
                }
            }
        }
    });

    // 2. ジャンル別売上比率円グラフ
    const ctxGenre = document.getElementById('genreChart').getContext('2d');
    if (genreChartInstance) {
        genreChartInstance.destroy();
    }

    // 期間フィルタの適用（商品タイプフィルタは無視して、ジャンル比率を出したい）
    const now = new Date();
    let startDate;
    switch (currentSummaryPeriod) {
        case 'monthly': startDate = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000); break;
        case 'weekly': startDate = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000); break;
        default: startDate = new Date('2000-01-01'); break;
    }

    const genreStats = {};
    globalAllRecords.forEach(record => {
        if (record.status === '取引完了') {
            const orderDate = new Date(record.orderDate.split(' ')[0]);
            if (orderDate >= startDate) {
                const type = record.productType || '未分類';
                if (!genreStats[type]) genreStats[type] = 0;
                genreStats[type] += (record.price || 0);
            }
        }
    });

    const genreLabels = Object.keys(genreStats);
    const genreData = Object.values(genreStats);
    
    // 色の生成
    const backgroundColors = [
        'rgba(255, 99, 132, 0.7)',
        'rgba(54, 162, 235, 0.7)',
        'rgba(255, 206, 86, 0.7)',
        'rgba(75, 192, 192, 0.7)',
        'rgba(153, 102, 255, 0.7)',
        'rgba(255, 159, 64, 0.7)'
    ];

    genreChartInstance = new Chart(ctxGenre, {
        type: 'doughnut',
        data: {
            labels: genreLabels,
            datasets: [{
                data: genreData,
                backgroundColor: backgroundColors,
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'bottom'
                }
            }
        }
    });
}

/**
 * ユーザー分析機能を描画します。
 */
function renderUserAnalysis() {
    // 現在のフィルタ（期間・商品タイプ）適用済みのレコードを対象にする
    let targetRecords = globalAllRecords;
    
    // 1. 商品タイプフィルタ
    if (currentProductTypeFilter !== 'all') {
        targetRecords = targetRecords.filter(r => (r.productType || '未分類') === currentProductTypeFilter);
    }

    // 2. 期間フィルタ
    const now = new Date();
    let startDate;
    switch (currentSummaryPeriod) {
        case 'monthly': startDate = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000); break;
        case 'weekly': startDate = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000); break;
        default: startDate = new Date('2000-01-01'); break;
    }

    const userStats = {};
    targetRecords.forEach(record => {
        const price = record.price || 0;
        // includeFreeProductsInAnalysis フラグに基づいてフィルタリング
        // trueなら price >= 0 (すべてOK)、falseなら price > 0 (有料のみ)
        const isTargetPrice = includeFreeProductsInAnalysis ? true : price > 0;

        if (record.status === '取引完了' && isTargetPrice) {
            const orderDate = new Date(record.orderDate.split(' ')[0]);
            if (orderDate >= startDate) {
                const uid = record.userId;
                if (!userStats[uid]) {
                    userStats[uid] = { totalRevenue: 0, count: 0, lastOrderDate: orderDate };
                }
                userStats[uid].totalRevenue += price;
                userStats[uid].count += 1;
                if (orderDate > userStats[uid].lastOrderDate) {
                    userStats[uid].lastOrderDate = orderDate;
                }
            }
        }
    });

    // 配列に変換してソート（売上順）
    const sortedUsers = Object.entries(userStats)
        .map(([uid, stat]) => ({ uid, ...stat }))
        .sort((a, b) => b.totalRevenue - a.totalRevenue)
        .slice(0, 10); // 上位10名を表示

    // リピーター分析
    const allUserCount = Object.keys(userStats).length; // 対象商品を1回以上購入したユーザー数
    const repeaters = Object.values(userStats).filter(u => u.count > 1).length; // 対象商品を2回以上購入したユーザー数
    const repeaterRate = allUserCount > 0 ? ((repeaters / allUserCount) * 100).toFixed(1) : 0;

    // HTML生成
    const userTableRows = sortedUsers.map((user, index) => `
        <tr class="border-b border-gray-100 hover:bg-gray-50">
            <td class="p-2 text-center font-bold text-gray-500">#${index + 1}</td>
            <td class="p-2 text-sm text-gray-700 font-mono">${user.uid}</td>
            <td class="p-2 text-right text-sm">${user.count}回</td>
            <td class="p-2 text-right text-sm font-bold text-green-600">${user.totalRevenue.toLocaleString()}円</td>
        </tr>
    `).join('');

    const userAnalysisHTML = `
        <div class="bg-white rounded-xl shadow-lg p-6 border border-gray-200 mb-6">
            <div class="flex flex-col sm:flex-row justify-between items-start sm:items-center mb-4 gap-4">
                <div>
                    <h2 class="text-lg font-bold text-gray-800 flex items-center">
                        ユーザー分析
                        <span class="text-xs font-normal text-gray-500 ml-2 py-0.5 px-2 bg-gray-100 rounded-full">
                            ${includeFreeProductsInAnalysis ? '無料商品含む' : '有料商品のみ'}
                        </span>
                    </h2>
                    <div class="flex items-center mt-2">
                        <input id="includeFreeCheckbox" type="checkbox" class="w-4 h-4 text-blue-600 bg-gray-100 border-gray-300 rounded focus:ring-blue-500" ${includeFreeProductsInAnalysis ? 'checked' : ''} onchange="window.toggleFreeProductsInAnalysis(this.checked)">
                        <label for="includeFreeCheckbox" class="ml-2 text-sm text-gray-600 cursor-pointer select-none">無料商品の購入者を含める</label>
                    </div>
                </div>
                <div class="bg-blue-50 px-4 py-2 rounded-lg self-stretch sm:self-auto text-center sm:text-right">
                    <span class="text-sm text-gray-600 mr-2">リピーター率:</span>
                    <span class="text-lg font-bold text-blue-600">${repeaterRate}%</span>
                    <div class="text-xs text-gray-500 mt-1">
                        (${repeaters} / ${allUserCount} 人)
                    </div>
                </div>
            </div>
            
            <div class="overflow-x-auto">
                <h3 class="text-sm font-semibold text-gray-500 mb-2">トップファンランキング (売上上位10名)</h3>
                <table class="w-full">
                    <thead class="bg-gray-50">
                        <tr>
                            <th class="p-2 text-center text-xs font-semibold text-gray-600">順位</th>
                            <th class="p-2 text-left text-xs font-semibold text-gray-600">ユーザーID</th>
                            <th class="p-2 text-right text-xs font-semibold text-gray-600">購入回数</th>
                            <th class="p-2 text-right text-xs font-semibold text-gray-600">購入総額</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${userTableRows.length > 0 ? userTableRows : '<tr><td colspan="4" class="text-center p-4 text-gray-500">データがありません</td></tr>'}
                    </tbody>
                </table>
            </div>
        </div>
    `;

    document.getElementById('userAnalysisArea').innerHTML = userAnalysisHTML;
}

/**
 * ユーザー分析の無料商品含めるフラグを切り替えます。
 */
window.toggleFreeProductsInAnalysis = (checked) => {
    includeFreeProductsInAnalysis = checked;
    renderUserAnalysis();
}

/**
 * 全体サマリーカードのHTMLを生成します。
 */
function createSummaryCard(totalRevenue, totalQuantity, productStats, genreLabel) {
  const uniqueProductCount = Object.keys(productStats).length;
  const periodLabel = currentSummaryPeriod === 'all' ? '全体' :
    currentSummaryPeriod === 'monthly' ? '月間' : '週間';

  return `
        <div class="bg-white rounded-xl shadow-lg p-6 border border-gray-200">
            <h2 class="text-xl font-bold text-gray-800 mb-4">${periodLabel}サマリー ${genreLabel} <span class="text-sm font-normal text-gray-500 ml-2">※取引完了のみ</span></h2>
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
 * 商品別レポートのHTMLを生成します。
 */
function createProductStatsTable(productStats) {
  const sortedProducts = Object.entries(productStats).sort(([, a], [, b]) => b.revenue - a.revenue);

  let tableRows = sortedProducts.map(([name, stats]) => {
    const avgPurchase = stats.uniqueUsers.size > 0 ? stats.quantity / stats.uniqueUsers.size : 0;
    
    // 商品別のアクティブユーザー数を取得
    const activeUsers = globalProductMetadata[name]?.activeUsers || '';
    
    // CVR計算 (購入者UU / アクティブユーザー * 100)
    let cvrDisplay = '-';
    if (activeUsers && activeUsers > 0) {
        const purchaseUU = stats.uniqueUsers.size;
        const cvr = (purchaseUU / activeUsers) * 100;
        cvrDisplay = cvr.toFixed(2) + '%';
    }
    
    // 商品名をエンコードして属性にセット（JSでの取得用）
    const encodedName = btoa(unescape(encodeURIComponent(name)));

    return `
                <tr class="border-b border-gray-200 hover:bg-gray-50" data-search-name="${name.toLowerCase()}">
                    <td class="p-3 text-sm text-gray-700 font-medium break-words max-w-xs">${name}</td>
                    <td class="p-3 text-center" onclick="event.stopPropagation()">
                        <input type="number" 
                            class="w-24 px-2 py-1 text-right border border-gray-300 rounded focus:ring-blue-500 focus:border-blue-500 text-sm" 
                            placeholder="UU入力" 
                            autocomplete="off"
                            value="${activeUsers}" 
                            data-product-encoded="${encodedName}"
                            onchange="window.saveProductActiveUsers(this)"
                            min="0">
                    </td>
                    <td class="p-3 text-right text-sm font-medium">${stats.uniqueUsers.size.toLocaleString()}</td>
                    <td class="p-3 text-right text-sm font-semibold text-blue-600">${cvrDisplay}</td>
                    <td class="p-3 text-right text-sm">${stats.quantity.toLocaleString()}</td>
                    <td class="p-3 text-right text-sm font-semibold text-green-600">${stats.revenue.toLocaleString()}円</td>
                    <td class="p-3 text-right text-xs text-gray-500">${avgPurchase.toFixed(2)}</td>
                </tr>
            `;
  }).join('');

  if (!tableRows) {
    tableRows = '<tr><td colspan="7" class="text-center p-4 text-gray-500">データがありません。</td></tr>';
  }

  return `
            <div class="bg-white rounded-xl shadow-lg p-6 border border-gray-200">
                <h2 class="text-xl font-bold text-gray-800 mb-4">商品別レポート</h2>
                <div class="overflow-x-auto max-h-[80vh]"> <!-- 縦並びにしたので高さを少し拡張 -->
                    <table id="productStatsTable" class="w-full min-w-[800px]"> <!-- 横幅を確保 -->
                        <thead class="bg-gray-50 sticky top-0 z-10">
                            <tr>
                                <th class="p-3 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider w-1/3">商品名</th>
                                <th class="p-3 text-center text-xs font-semibold text-gray-600 uppercase tracking-wider">アクティブ<br>ユーザー</th>
                                <th class="p-3 text-right text-xs font-semibold text-gray-600 uppercase tracking-wider">購入者数</th>
                                <th class="p-3 text-right text-xs font-semibold text-gray-600 uppercase tracking-wider">CVR</th>
                                <th class="p-3 text-right text-xs font-semibold text-gray-600 uppercase tracking-wider">販売個数</th>
                                <th class="p-3 text-right text-xs font-semibold text-gray-600 uppercase tracking-wider">売上</th>
                                <th class="p-3 text-right text-xs font-semibold text-gray-600 uppercase tracking-wider">平均/人</th>
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
 * アクティブユーザー入力欄とCVRを追加。
 */
function createDailyStatsTable(dailyStats) {
  const sortedDates = Object.keys(dailyStats).sort((a, b) => new Date(b) - new Date(a));

  let tableRows = sortedDates.map(date => {
    // アクティブユーザー数の取得（メタデータから）
    const activeUsers = globalDailyMetadata[date]?.activeUsers || '';
    // 購入者UU数
    const purchaseUU = dailyStats[date].uniqueUsers.size;
    
    // CVR計算 (購入UU / アクティブユーザー * 100)
    let cvrDisplay = '-';
    if (activeUsers && activeUsers > 0) {
        const cvr = (purchaseUU / activeUsers) * 100;
        cvrDisplay = cvr.toFixed(2) + '%';
    }

    return `
            <tr class="border-b border-gray-200 hover:bg-gray-50 cursor-pointer" onclick="window.showDailyDetailsModal('${date}')">
                <td class="p-3 text-sm text-gray-700 whitespace-nowrap">${date}</td>
                <td class="p-3 text-center" onclick="event.stopPropagation()">
                    <input type="number" 
                           class="w-24 px-2 py-1 text-right border border-gray-300 rounded focus:ring-blue-500 focus:border-blue-500 text-sm" 
                           placeholder="UU入力" 
                           autocomplete="off"
                           value="${activeUsers}" 
                           onchange="window.saveActiveUsers('${date}', this.value)"
                           min="0">
                </td>
                <td class="p-3 text-right text-sm font-medium">${purchaseUU.toLocaleString()}</td>
                <td class="p-3 text-right text-sm font-semibold text-blue-600">${cvrDisplay}</td>
                <td class="p-3 text-right font-medium text-sm">${dailyStats[date].quantity.toLocaleString()}</td>
                <td class="p-3 text-right font-semibold text-green-600 text-sm">${dailyStats[date].revenue.toLocaleString()}円</td>
            </tr>
        `;
  }).join('');

  if (!tableRows) {
    tableRows = '<tr><td colspan="6" class="text-center p-4 text-gray-500">データがありません。</td></tr>';
  }

  return `
            <div class="bg-white rounded-xl shadow-lg p-6 border border-gray-200">
                <h2 class="text-xl font-bold text-gray-800 mb-4">日別レポート (クリックで詳細)</h2>
                <div class="overflow-x-auto max-h-[80vh]"> <!-- 縦並びにしたので高さを少し拡張 -->
                    <table class="w-full min-w-[600px]">
                        <thead class="bg-gray-50 sticky top-0 z-10">
                            <tr>
                                <th class="p-3 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">日付</th>
                                <th class="p-3 text-center text-xs font-semibold text-gray-600 uppercase tracking-wider">アクティブ<br>ユーザー</th>
                                <th class="p-3 text-right text-xs font-semibold text-gray-600 uppercase tracking-wider">購入者<br>(UU)</th>
                                <th class="p-3 text-right text-xs font-semibold text-gray-600 uppercase tracking-wider">CVR</th>
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
 * 日別アクティブユーザー数を保存し、CVRを再計算します。
 */
window.saveActiveUsers = async (date, value) => {
  const numValue = parseInt(value, 10);
  
  if (isNaN(numValue) || numValue < 0) {
      if (value !== '') alert("有効な数値を入力してください");
      return;
  }

  const companyGroupId = companyGroupSelector.value;
  const castId = castSelector.value;
  
  if (!companyGroupId || !castId) return;

  try {
      // グローバルデータを即時更新してUIに反映（UX向上）
      if (!globalDailyMetadata[date]) globalDailyMetadata[date] = {};
      globalDailyMetadata[date].activeUsers = numValue;
      
      // ビューを更新 (CVR再計算のため)
      updateViewFromGlobalData();

      // Firestoreに保存
      const metadataDocRef = doc(db, `artifacts/${appId}/public/data/companyGroups/${companyGroupId}/casts/${castId}/dailyMetadata/${date}`);
      await setDoc(metadataDocRef, { activeUsers: numValue }, { merge: true });
      
      console.log(`${date} のアクティブユーザー数を保存: ${numValue}`);

  } catch (error) {
      console.error("アクティブユーザー数の保存に失敗:", error);
      alert("保存に失敗しました。");
  }
};

/**
 * 商品別アクティブユーザー数を保存し、CVRを再計算します。
 */
window.saveProductActiveUsers = async (inputElement) => {
  const value = inputElement.value;
  const encodedName = inputElement.getAttribute('data-product-encoded');
  // Base64デコード -> URIデコード で元の日本語商品名に戻す
  const productName = decodeURIComponent(escape(atob(encodedName)));

  const numValue = parseInt(value, 10);
  
  if (isNaN(numValue) || numValue < 0) {
      if (value !== '') alert("有効な数値を入力してください");
      return;
  }

  const companyGroupId = companyGroupSelector.value;
  const castId = castSelector.value;
  
  if (!companyGroupId || !castId) return;

  try {
      // グローバルデータを即時更新
      if (!globalProductMetadata[productName]) globalProductMetadata[productName] = {};
      globalProductMetadata[productName].activeUsers = numValue;
      
      // ビューを更新
      updateViewFromGlobalData();

      // Firestoreに保存 (ドキュメントIDは安全のためハッシュ化した方が良いが、
      // 読み込み時のマッピング簡易化のため今回はSHA-1ハッシュをIDとし、データ内に商品名を含める)
      const hashBuffer = await crypto.subtle.digest('SHA-1', new TextEncoder().encode(productName));
      const hashArray = Array.from(new Uint8Array(hashBuffer));
      const docId = hashArray.map(b => b.toString(16).padStart(2, '0')).join('');

      const productMetaDocRef = doc(db, `artifacts/${appId}/public/data/companyGroups/${companyGroupId}/casts/${castId}/productMetadata/${docId}`);
      await setDoc(productMetaDocRef, { 
          productName: productName,
          activeUsers: numValue 
      }, { merge: true });
      
      console.log(`商品「${productName}」のアクティブユーザー数を保存: ${numValue}`);

  } catch (error) {
      console.error("商品別アクティブユーザー数の保存に失敗:", error);
      alert("保存に失敗しました。");
  }
};


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
    alert("開始日と終了日を両方選択してください。");
    return;
  }

  const startDate = new Date(startDateStr);
  const endDate = new Date(endDateStr);

  if (startDate > endDate) {
    alert("終了日は開始日より後の日付を選択してください。");
    return;
  }
  endDate.setHours(23, 59, 59, 999);

  // 1. まず現在のフィルタ(商品タイプ)で絞り込む
  let targetRecords = globalAllRecords;
  if (currentProductTypeFilter !== 'all') {
    targetRecords = globalAllRecords.filter(r => (r.productType || '未分類') === currentProductTypeFilter);
  }

  // 2. その中から期間で集計
  let rangeTotalRevenue = 0;
  let rangeTotalQuantity = 0;
  const rangeProductStats = {};
  const uniqueUsers = new Set(); // 全体のユニークユーザー数

  for (const record of targetRecords) {
    if (record.status === '取引完了') {
      const orderDate = new Date(record.orderDate.split(' ')[0]);
      if (orderDate >= startDate && orderDate <= endDate) {
        rangeTotalRevenue += record.price || 0;
        rangeTotalQuantity += record.quantity || 0;
        uniqueUsers.add(record.userId);

        const productName = record.productName;
        if (!rangeProductStats[productName]) {
          rangeProductStats[productName] = { 
              quantity: 0, 
              revenue: 0,
              uniqueUsers: new Set() // 商品ごとのユニークユーザー数
          };
        }
        rangeProductStats[productName].quantity += (record.quantity || 0);
        rangeProductStats[productName].revenue += (record.price || 0);
        rangeProductStats[productName].uniqueUsers.add(record.userId);
      }
    }
  }

  const userCount = uniqueUsers.size;
  const avgPurchasePerUser = userCount > 0 ? (rangeTotalQuantity / userCount).toFixed(2) : '0.00';

  updateRangeSummaryModal(rangeTotalRevenue, rangeTotalQuantity, avgPurchasePerUser, rangeProductStats);
}


function updateRangeSummaryModal(rangeTotalRevenue, rangeTotalQuantity, avgPurchasePerUser, rangeProductStats) {
  let genreText = currentProductTypeFilter === 'all' ? '' : `<span class="text-sm font-normal ml-2">(${currentProductTypeFilter})</span>`;

  let modalHTML = `
             <div class="bg-blue-50 rounded-lg p-4 mb-6">
                 <h3 class="text-lg font-bold text-gray-800 mb-3">期間サマリー${genreText}</h3>
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

  const sortedProducts = Object.entries(rangeProductStats).sort(([, a], [, b]) => b.revenue - a.revenue);

  // くじ対応: 単価、UU、販売個数、売上を表示
  const productRows = sortedProducts.map(([name, stats]) => {
     // 単価（平均） = 売上 / 販売個数
     const unitPrice = stats.quantity > 0 ? Math.round(stats.revenue / stats.quantity) : 0;
     const purchaseUU = stats.uniqueUsers.size;

     return `
             <tr class="border-t border-gray-200" data-search-name="${name.toLowerCase()}">
                 <td class="p-3 text-sm text-gray-600 break-words max-w-xs">${name}</td>
                 <td class="p-3 text-right text-sm text-gray-800">${unitPrice.toLocaleString()}円</td>
                 <td class="p-3 text-right text-sm font-medium text-blue-600">${purchaseUU.toLocaleString()}人</td>
                 <td class="p-3 text-right font-medium">${stats.quantity.toLocaleString()}</td>
                 <td class="p-3 text-right text-green-600 font-bold">${stats.revenue.toLocaleString()}円</td>
             </tr>
         `;
  }).join('');

  modalHTML += `
             <h3 class="text-lg font-bold text-gray-800 mb-3">期間中の商品別売上詳細</h3>
             <div class="overflow-x-auto">
                <table id="modalRangeProductStatsTable" class="w-full text-sm">
                    <thead class="border-b bg-gray-50">
                        <tr>
                            <th class="p-3 text-left font-semibold text-gray-600 w-1/3">商品名</th>
                            <th class="p-3 text-right font-semibold text-gray-600">単価(平均)</th>
                            <th class="p-3 text-right font-semibold text-gray-600">購入者数(UU)</th>
                            <th class="p-3 text-right font-semibold text-gray-600">販売個数</th>
                            <th class="p-3 text-right font-semibold text-gray-600">売上</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${productRows.length > 0 ? productRows : '<tr><td colspan="5" class="text-center p-4 text-gray-500">該当期間にデータはありません。</td></tr>'}
                    </tbody>
                </table>
             </div>
        `;

  rangeModalBody.innerHTML = modalHTML;
  showRangeSummaryModal();
}

/**
 * 検索フィルターを適用します。
 */
function applySearchFilter() {
  if (!searchInput) return;
  const query = searchInput.value.toLowerCase().trim();

  const productTable = document.getElementById('productStatsTable');
  if (productTable) {
    const rows = productTable.getElementsByTagName('tbody')[0].getElementsByTagName('tr');
    for (const row of rows) {
      const name = row.dataset.searchName;
      if (name) {
        row.style.display = name.includes(query) ? '' : 'none';
      }
    }
  }

  const modalTable = document.getElementById('modalProductStatsTable');
  if (modalTable && !dailyDetailsModal.classList.contains('opacity-0')) {
    const modalRows = modalTable.getElementsByTagName('tbody')[0].getElementsByTagName('tr');
    for (const row of modalRows) {
      const name = row.dataset.searchName;
      if (name) {
        row.style.display = name.includes(query) ? '' : 'none';
      }
    }
  }
    
  const rangeModalTable = document.getElementById('modalRangeProductStatsTable');
  if (rangeModalTable && !rangeSummaryModal.classList.contains('opacity-0')) {
    const modalRows = rangeModalTable.getElementsByTagName('tbody')[0].getElementsByTagName('tr');
    for (const row of modalRows) {
      const name = row.dataset.searchName;
      if (name) {
         row.style.display = name.includes(query) ? '' : 'none';
      }
    }
  }
}

// --- モーダル制御 ---

window.showDailyDetailsModal = (date) => {
  // globalDailyStatsは現在選択されているタブ（ジャンル）に基づいてフィルタリングされたデータ
  const data = globalDailyStats[date];
  if (!data) return;

  modalTitle.textContent = `${date} の商品別レポート`;
  // ジャンル名も表示したければ: modalTitle.textContent += ` (${currentProductTypeFilter === 'all' ? '全体' : currentProductTypeFilter})`;

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
                        <th class="pb-2 text-right font-semibold text-gray-600">販売個数</th>
                        <th class="pb-2 text-right font-semibold text-gray-600">売上</th>
                    </tr>
                </thead>
                <tbody>
                    ${productRows}
                </tbody>
            </table>
        `;

  dailyDetailsModal.classList.remove('opacity-0', 'pointer-events-none');
  dailyDetailsModal.firstElementChild.classList.remove('scale-95');
  applySearchFilter();
}

window.hideDailyDetailsModal = () => {
  dailyDetailsModal.classList.add('opacity-0', 'pointer-events-none');
  dailyDetailsModal.firstElementChild.classList.add('scale-95');
}

window.showRangeSummaryModal = () => {
  rangeSummaryModal.classList.remove('opacity-0', 'pointer-events-none');
  rangeSummaryModal.firstElementChild.classList.remove('scale-95');
  applySearchFilter();
}

window.hideRangeSummaryModal = () => {
  rangeSummaryModal.classList.add('opacity-0', 'pointer-events-none');
  rangeSummaryModal.firstElementChild.classList.add('scale-95');
}

document.addEventListener('DOMContentLoaded', initializeMainApp);
