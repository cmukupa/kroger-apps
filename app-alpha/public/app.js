const storeSelect = document.getElementById("store");
const details = document.getElementById("details");

async function loadStores() {
  const res = await fetch("/api/stores");
  if (!res.ok) throw new Error(`stores request failed: ${res.status}`);
  const data = await res.json();

  for (const store of data.items) {
    const option = document.createElement("option");
    option.value = store.id;
    option.textContent = store.name;
    storeSelect.appendChild(option);
  }
}

storeSelect.addEventListener("change", () => {
  const selected = storeSelect.options[storeSelect.selectedIndex];
  details.textContent = storeSelect.value
    ? `Selected: ${selected.textContent} (${storeSelect.value})`
    : "No store selected.";
});

loadStores().catch((err) => console.error("Could not load stores", err));
