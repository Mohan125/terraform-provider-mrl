package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
)

// Ensure the implementation satisfies the expected interfaces.
var (
	_ datasource.DataSource              = &DatabricksDbfsSource{}
	_ datasource.DataSourceWithConfigure = &DatabricksDbfsSource{}
)

// NewCoffeesDataSource is a helper function to simplify the provider implementation.
func NewDatabricksDbfs() datasource.DataSource {
	return &DatabricksDbfsSource{}
}

// coffeesDataSource is the data source implementation.
type DatabricksDbfsSource struct {
	credential *azidentity.ClientSecretCredential
}

// Configure implements datasource.DataSourceWithConfigure.
// Configure adds the provider configured client to the data source.
func (d *DatabricksDbfsSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}

	credential, ok := req.ProviderData.(*azidentity.ClientSecretCredential)
	if !ok {
		resp.Diagnostics.AddError(
			"Unexpected Data Source Configure Type",
			fmt.Sprintf("Expected *hashicups.Client, got: %T. Please report this issue to the provider developers.", req.ProviderData),
		)

		return
	}

	d.credential = credential
}

// Metadata returns the data source type name.
func (d *DatabricksDbfsSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_databricks_dbfs"
}

// Schema defines the schema for the data source.
// Schema defines the schema for the data source.
func (d *DatabricksDbfsSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Attributes: map[string]schema.Attribute{
			"adb_id": schema.StringAttribute{
				Required:    true,
				Sensitive:   true,
				Description: "URL of the azure databricks instance",
			},
			"token": schema.StringAttribute{
				Required:    true,
				Sensitive:   true,
				Description: "Access token for the azure databricks instance",
			},
			"root_path": schema.StringAttribute{
				Required:    true,
				Description: "Local path from where the file needs to be read",
			},
			"files": schema.ListNestedAttribute{
				Computed: true,
				NestedObject: schema.NestedAttributeObject{
					Attributes: map[string]schema.Attribute{
						"path": schema.StringAttribute{
							Optional:    true,
							Description: "Path in dbfs where the file is present",
						},
						"is_dir": schema.BoolAttribute{
							Optional:    true,
							Description: "Type of the path dir/file",
						},
						"file_size": schema.Int64Attribute{
							Optional:    true,
							Description: "Size of the file being managed",
						},
						"modification_time": schema.StringAttribute{
							Optional:    true,
							Description: "Last modified time of the file being managed",
						},
					},
				},
			},
		},
	}
}

// coffeesDataSourceModel maps the data source schema data.
type databricksDbfsDataSourceModel struct {
	AdbId    string           `tfsdk:"adb_id"`
	Token    string           `tfsdk:"token"`
	RootPath string           `tfsdk:"root_path"`
	Files    []dbfsFilesModel `tfsdk:"files"`
}

// coffeesModel maps coffees schema data.
type dbfsFilesModel struct {
	Path         types.String `tfsdk:"path"`
	IsDirectory  types.Bool   `tfsdk:"is_dir"`
	FileSize     types.Int64  `tfsdk:"file_size"`
	LastModified types.String `tfsdk:"modification_time"`
}

// Read refreshes the Terraform state with the latest data.
// Read refreshes the Terraform state with the latest data.
func (d *DatabricksDbfsSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var state databricksDbfsDataSourceModel
	diags := req.Config.Get(ctx, &state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	adburl := state.AdbId
	token := state.Token
	path := state.RootPath
	endpoint := fmt.Sprintf("%v/api/2.0/dbfs/list?path=%v", adburl, path)

	client := http.Client{}
	getFilesHttpRequest, _ := http.NewRequest("GET", endpoint, nil)

	getFilesHttpRequest.Header = http.Header{
		"Authorization": {fmt.Sprintf("Bearer %v", token)},
	}

	getFilesHttpResult, _ := client.Do(getFilesHttpRequest)

	body, _ := io.ReadAll(getFilesHttpResult.Body)

	fmt.Println(json.Valid(body))

	getFilesHttpResponse := struct {
		Files []struct {
			Path         string `json:"path"`
			IsDirectory  bool   `json:"is_dir"`
			FileSize     int64  `json:"file_size"`
			LastModified int    `json:"modification_time"`
		} `json:"files"`
	}{}

	err := json.Unmarshal(body, &getFilesHttpResponse)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error creating order",
			"Could not create order, unexpected error: "+err.Error(),
		)
		return
	}
	for i := 0; i < len(getFilesHttpResponse.Files); i++ {
		dbfsFile := dbfsFilesModel{
			Path:         types.StringValue(getFilesHttpResponse.Files[i].Path),
			IsDirectory:  types.BoolValue(getFilesHttpResponse.Files[i].IsDirectory),
			FileSize:     types.Int64Value(getFilesHttpResponse.Files[i].FileSize),
			LastModified: types.StringValue(time.UnixMilli(int64(getFilesHttpResponse.Files[i].LastModified)).UTC().Format(time.RFC3339)),
		}
		fmt.Println(dbfsFile)
		tflog.Info(ctx, "Configuring HashiCups client")
		state.Files = append(state.Files, dbfsFile)
	}

	diags = resp.State.Set(ctx, &state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
}
