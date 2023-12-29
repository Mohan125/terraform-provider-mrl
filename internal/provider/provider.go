package provider

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

// Ensure the implementation satisfies the expected interfaces.
var (
	_ provider.Provider = &mrlProvider{}
)

// New is a helper function to simplify provider server and testing implementation.
func New(version string) func() provider.Provider {
	return func() provider.Provider {
		return &mrlProvider{
			version: version,
		}
	}
}

// mrlProvider is the provider implementation.
type mrlProvider struct {
	// version is set to the provider version on release, "dev" when the
	// provider is built and ran locally, and "test" when running acceptance
	// testing.
	version string
}
type mrlProviderModel struct {
	ClientId       types.String `tfsdk:"clientid"`
	ClientSecret   types.String `tfsdk:"clientsecret"`
	SubscriptionId types.String `tfsdk:"subscriptionid"`
	TenantId       types.String `tfsdk:"tenantid"`
}

// Metadata returns the provider type name.
func (p *mrlProvider) Metadata(_ context.Context, _ provider.MetadataRequest, resp *provider.MetadataResponse) {
	resp.TypeName = "mrl"
	resp.Version = p.version
}

// Schema defines the provider-level schema for configuration data.
func (p *mrlProvider) Schema(_ context.Context, _ provider.SchemaRequest, resp *provider.SchemaResponse) {
	resp.Schema = schema.Schema{
		Attributes: map[string]schema.Attribute{
			"clientid": schema.StringAttribute{
				Optional:    true,
				Description: "Provide the clientid of the spn which has permission to do the necessary resource creation",
			},
			"clientsecret": schema.StringAttribute{
				Optional:    true,
				Description: "Provide the clientsecret of the spn which has permission to do the necessary resource creation",
			},
			"subscriptionid": schema.StringAttribute{
				Optional:    true,
				Sensitive:   true,
				Description: "Provide the subscriptionid id of the subscription in which the resources needs to be created",
			},
			"tenantid": schema.StringAttribute{
				Optional:    true,
				Sensitive:   true,
				Description: "Provide the tenant id of the tenant in which the resources needs to be created",
			},
		},
	}
}

// Configure prepares a mrl API client for data sources and resources.
func (p *mrlProvider) Configure(ctx context.Context, req provider.ConfigureRequest, resp *provider.ConfigureResponse) {
	// Retrieve provider data from configuration
	var config mrlProviderModel
	diags := req.Config.Get(ctx, &config)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	// If practitioner provided a configuration value for any of the
	// attributes, it must be a known value.

	if config.ClientId.IsUnknown() {
		resp.Diagnostics.AddAttributeError(
			path.Root("clientid"),
			"Unknown HashiCups API clientId",
			"The provider cannot create the HashiCups API client as there is an unknown configuration value for the HashiCups API host. "+
				"Either target apply the source of the value first, set the value statically in the configuration, or use the HASHICUPS_HOST environment variable.",
		)
	}

	if config.ClientSecret.IsUnknown() {
		resp.Diagnostics.AddAttributeError(
			path.Root("clientsecret"),
			"Unknown HashiCups API clientSecret",
			"The provider cannot create the HashiCups API client as there is an unknown configuration value for the HashiCups API username. "+
				"Either target apply the source of the value first, set the value statically in the configuration, or use the HASHICUPS_USERNAME environment variable.",
		)
	}

	if config.SubscriptionId.IsUnknown() {
		resp.Diagnostics.AddAttributeError(
			path.Root("subscriptionid"),
			"Unknown HashiCups API subscriptionId",
			"The provider cannot create the HashiCups API client as there is an unknown configuration value for the HashiCups API password. "+
				"Either target apply the source of the value first, set the value statically in the configuration, or use the HASHICUPS_PASSWORD environment variable.",
		)
	}

	if config.TenantId.IsUnknown() {
		resp.Diagnostics.AddAttributeError(
			path.Root("tenantid"),
			"Unknown HashiCups API tenantId",
			"The provider cannot create the HashiCups API client as there is an unknown configuration value for the HashiCups API password. "+
				"Either target apply the source of the value first, set the value statically in the configuration, or use the HASHICUPS_PASSWORD environment variable.",
		)
	}

	if resp.Diagnostics.HasError() {
		return
	}

	// Default values to environment variables, but override
	// with Terraform configuration value if set.

	// host := os.Getenv("HASHICUPS_HOST")
	// username := os.Getenv("HASHICUPS_USERNAME")
	// password := os.Getenv("HASHICUPS_PASSWORD")

	clientid := config.ClientId.ValueString()
	clientsecret := config.ClientSecret.ValueString()
	subscriptionid := config.SubscriptionId.ValueString()
	tenantid := config.TenantId.ValueString()

	fmt.Println(subscriptionid)

	// // If any of the expected configurations are missing, return
	// // errors with provider-specific guidance.

	if clientid == "" {
		resp.Diagnostics.AddAttributeError(
			path.Root("clientid"),
			"Missing clientid",
			"The provider cannot create the HashiCups API client as there is a missing or empty value for the HashiCups API host. "+
				"Set the host value in the configuration or use the HASHICUPS_HOST environment variable. "+
				"If either is already set, ensure the value is not empty.",
		)
	}

	if clientsecret == "" {
		resp.Diagnostics.AddAttributeError(
			path.Root("clientsecret"),
			"Missing clientSecret",
			"The provider cannot create the HashiCups API client as there is a missing or empty value for the HashiCups API username. "+
				"Set the username value in the configuration or use the HASHICUPS_USERNAME environment variable. "+
				"If either is already set, ensure the value is not empty.",
		)
	}

	if subscriptionid == "" {
		resp.Diagnostics.AddAttributeError(
			path.Root("subscriptionid"),
			"Missing subscriptionid",
			"The provider cannot create the HashiCups API client as there is a missing or empty value for the HashiCups API password. "+
				"Set the password value in the configuration or use the HASHICUPS_PASSWORD environment variable. "+
				"If either is already set, ensure the value is not empty.",
		)
	}

	if tenantid == "" {
		resp.Diagnostics.AddAttributeError(
			path.Root("tenantid"),
			"Missing tenantid",
			"The provider cannot create the HashiCups API client as there is a missing or empty value for the HashiCups API password. "+
				"Set the password value in the configuration or use the HASHICUPS_PASSWORD environment variable. "+
				"If either is already set, ensure the value is not empty.",
		)
	}

	if resp.Diagnostics.HasError() {
		return
	}

	// Create a new HashiCups client using the configuration values
	credential, err := azidentity.NewClientSecretCredential(tenantid, clientid, clientsecret, nil)
	if err != nil {
		resp.Diagnostics.AddError(
			"Unable to Create Credentials",
			"An unexpected error occurred when creating the HashiCups API client. "+
				"If the error is not clear, please contact the provider developers.\n\n"+
				"HashiCups Client Error: "+err.Error(),
		)
		return
	}

	// Make the HashiCups client available during DataSource and Resource
	// type Configure methods.
	resp.DataSourceData = credential
	resp.ResourceData = credential
}

// DataSources defines the data sources implemented in the provider.
// DataSources defines the data sources implemented in the provider.
func (p *mrlProvider) DataSources(_ context.Context) []func() datasource.DataSource {
	return []func() datasource.DataSource{
		NewDatabricksDbfs,
	}
}

// Resources defines the resources implemented in the provider.
func (p *mrlProvider) Resources(_ context.Context) []func() resource.Resource {
	return []func() resource.Resource{
		NewDatabricksDbfsResource,
	}
}
