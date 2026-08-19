export type ProviderConnectionBudgetPreview = Readonly<{
    providerLimit: number;
    transferLimit: number;
    baseMetadataCapacity: number;
    metadataBurstAllowance: number;
    maxMetadataCapacity: number;
}>;

export function calculateProviderConnectionBudget(
    providerLimit: number,
    transferLimit: number,
): ProviderConnectionBudgetPreview | null {
    if (!Number.isInteger(providerLimit)
        || providerLimit <= 0
        || !Number.isInteger(transferLimit)
        || transferLimit <= 0
        || transferLimit > providerLimit) {
        return null;
    }

    const baseMetadataCapacity = providerLimit - transferLimit;
    const metadataBurstAllowance = Math.floor(transferLimit / 2);

    return {
        providerLimit,
        transferLimit,
        baseMetadataCapacity,
        metadataBurstAllowance,
        maxMetadataCapacity: baseMetadataCapacity + metadataBurstAllowance,
    };
}

export function formatMetadataCapacity(
    budget: Pick<ProviderConnectionBudgetPreview, "baseMetadataCapacity" | "maxMetadataCapacity">,
): string {
    return `${budget.baseMetadataCapacity}\u2013${budget.maxMetadataCapacity} connections`;
}
